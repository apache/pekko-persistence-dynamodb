/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb.journal

import org.apache.pekko
import pekko.NotUsed
import pekko.actor.ExtendedActorSystem
import pekko.dispatch.MessageDispatcher
import pekko.persistence.PersistentRepr
import pekko.persistence.dynamodb._
import pekko.serialization.{ AsyncSerializer, Serialization }
import pekko.stream._
import pekko.stream.scaladsl._
import pekko.stream.stage._
import com.amazonaws.services.dynamodbv2.model._

import java.util.function.Consumer
import java.util.{ ArrayList, Collections, Map => JMap }
import scala.collection.immutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

object DynamoDBRecovery {
  val ItemAttributesForReplay: Seq[String] = Seq(
    Key,
    Sort,
    PersistentId,
    SequenceNr,
    Payload,
    Event,
    Manifest,
    SerializerId,
    SerializerManifest,
    WriterUuid,
    AtomEnd,
    AtomIndex)

  case class ReplayBatch(items: Seq[Item], map: Map[AttributeValue, Long]) {
    def sorted: immutable.Iterable[Item] =
      items.foldLeft(immutable.TreeMap.empty[Long, Item])((acc, i) => acc.updated(itemToSeq(i), i)).map(_._2)
    def ids: Seq[Long] = items.map(itemToSeq).sorted
    private def itemToSeq(i: Item): Long = map(i.get(Key)) * PartitionSize + i.get(Sort).getN.toInt
  }
}

/**
 * A simple data structure representing a Partition Key sequence number and the event numbers contained within it.
 *
 * @param partitionSeqNum - the partition sequence number for the given persistence id.
 * @param partitionEventNums - will be 0-99, representing the event ordering within the given partition sequence.
 */
case class PartitionKeys(partitionSeqNum: Long, partitionEventNums: immutable.Seq[Long])

/**
 * Groups Longs from a stream into a [PartitionKeys] whereas each sequence shall contain the values that would be within the
 * given partition size (represented by n)
 */
object DynamoPartitionGrouped extends GraphStage[FlowShape[Long, PartitionKeys]] {

  val in = Inlet[Long]("DynamoEventNum.in")
  val out = Outlet[PartitionKeys]("DynamoPartitionKeys.out")

  override val initialAttributes = Attributes.name("DynamoPartitionGrouped")

  override val shape: FlowShape[Long, PartitionKeys] = FlowShape(in, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val partitionBuf = {
        val b = Vector.newBuilder[Long]
        b.sizeHint(PartitionSize)
        b
      }
      var hasElements = false

      def pushOut(currentSeqNo: Long, partitionGroup: Vector[Long]): Unit = {
        partitionBuf.clear()
        hasElements = false
        val partitionSeqNo = currentSeqNo / PartitionSize
        push(out, PartitionKeys(partitionSeqNo, partitionGroup))
      }

      override def onPush(): Unit = {
        val currentSeqNo = grab(in)
        partitionBuf += currentSeqNo
        hasElements = true

        // If the next entry received would result in the next partition, then we clear the buf and push the results out
        if ((currentSeqNo + 1) % PartitionSize == 0) {
          val partitionGroup = partitionBuf.result()
          pushOut(currentSeqNo, partitionGroup)
        } else {
          pull(in)
        }
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        // this means the partitionBuf has elements but not a full amount (n). However, since upstream is finished
        // publishing elements, we need to push what we have downstream.
        if (hasElements) {
          val partitionGroup = partitionBuf.result()
          val currentSeqNo = partitionGroup.last
          pushOut(currentSeqNo, partitionGroup)
        }
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}

object RemoveIncompleteAtoms extends GraphStage[FlowShape[Item, List[Item]]] {
  private final val NoBatch = -1L

  val in = Inlet[Item]("RIA.in")
  val out = Outlet[List[Item]]("RIA.out")

  override val shape = FlowShape(in, out)
  override val initialAttributes = Attributes.name("RemoveIncompleteAtoms")

  override def createLogic(attr: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      var batchEnd = NoBatch
      var batch = List.empty[Item]

      setHandler(out, this)
      setHandler(in, this)

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = {
        val item = grab(in)
        if (item.containsKey(AtomEnd)) {
          val end = item.get(AtomEnd).getN.toLong
          val index = item.get(AtomIndex).getN.toLong
          val seqNr = sn(item)
          val myBatchEnd = seqNr - index + end
          if (seqNr == batchEnd) {
            val result =
              if (myBatchEnd == batchEnd) {
                val r = (item :: batch).reverse
                batch = Nil
                batchEnd = NoBatch
                r
              } else {
                // foul play detected, scrap this batch
                batch = item :: Nil
                batchEnd = myBatchEnd
                Nil
              }
            if (result.size == (end + 1)) push(out, result)
            else pull(in)
          } else if (batchEnd == NoBatch || seqNr > batchEnd) {
            batchEnd = myBatchEnd
            batch = item :: Nil
            pull(in)
          } else {
            if (batchEnd == myBatchEnd) batch ::= item
            else {
              batchEnd = myBatchEnd
              batch = item :: Nil
            }
            pull(in)
          }
        } else {
          push(out, item :: Nil)
          // throw away possible incomplete batch
          batchEnd = NoBatch
          batch = Nil
        }
      }

      private def sn(item: Item): Long = {
        val s = item.get(Key).getS
        val n = item.get(Sort).getN.toLong
        val pos = s.lastIndexOf('-')
        require(pos != -1, "unknown key format " + s)
        s.substring(pos + 1).toLong * PartitionSize + n
      }

    }
}
trait AsyncReplayMessages {
  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit): Future[Unit]

}

trait DynamoDBRecovery extends AsyncReplayMessages {
  self: DynamoProvider
    with JournalSettingsProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider =>
  import DynamoDBRecovery._
  import journalSettings._

  implicit lazy val replayDispatcher: MessageDispatcher = system.dispatchers.lookup(ReplayDispatcher)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    logFailure(s"replay for $persistenceId ($fromSequenceNr to $toSequenceNr)") {
      log.debug("starting replay for {} from {} to {} (max {})", persistenceId, fromSequenceNr, toSequenceNr, max)

      eventsStream(
        persistenceId = persistenceId,
        fromSequenceNr = fromSequenceNr,
        toSequenceNr = toSequenceNr,
        max = max)
        .runFold(0) { (count, next) => replayCallback(next); count + 1 }
        .map(count => log.debug("replay finished for {} with {} events", persistenceId, count))
    }
  }

  def eventsStream(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long): Source[PersistentRepr, NotUsed] =
    // toSequenceNr is already capped to highest and guaranteed to be no less than fromSequenceNr
    Source.future(readSequenceNr(persistenceId, highest = false)).flatMapConcat { lowest =>
      val start = Math.max(fromSequenceNr, lowest)
      val async = ReplayParallelism > 1

      Source(start to toSequenceNr)
        .via(DynamoPartitionGrouped)
        .mapAsync(ReplayParallelism)(batch => getPartitionItems(persistenceId, batch).map(_.sorted))
        .mapConcat(identity)
        .take(max)
        .via(RemoveIncompleteAtoms)
        .mapConcat(identity)
        .mapAsync(ReplayParallelism)(readPersistentRepr(_, async))
    }

  def getPartitionItems(persistenceId: String, partitionKeys: PartitionKeys): Future[ReplayBatch] = {
    val sortedNrs = partitionKeys.partitionEventNums.sorted.map(_ % PartitionSize)
    val startSortKey = sortedNrs.head
    val endSortKey = sortedNrs.last

    val queryRequestBuilder: (Option[java.util.Map[String, AttributeValue]]) => QueryRequest = exclusiveStartKeyOpt => {
      val request = new QueryRequest()
        .withTableName(JournalTable)
        .withKeyConditionExpression(s"$Key = :kkey AND $Sort BETWEEN :startSKey AND :endSKey")
        .withExpressionAttributeValues(
          Map(
            ":kkey" -> S(messagePartitionKeyFromGroupNr(persistenceId, partitionKeys.partitionSeqNum)),
            ":startSKey" -> N(startSortKey),
            ":endSKey" -> N(endSortKey)).asJava)
        .withProjectionExpression(ItemAttributesForReplay.mkString(","))
        .withConsistentRead(true)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      exclusiveStartKeyOpt.foreach(startKey => request.withExclusiveStartKey(startKey))
      request
    }

    def dynamoSummingPager(queryReq: QueryRequest, acc: Seq[Item]): Future[Seq[Item]] = {
      dynamo.query(queryReq).flatMap { result =>
        val currentPageItems = result.getItems.asScala.toSeq
        if (result.getLastEvaluatedKey == null || result.getLastEvaluatedKey.isEmpty)
          Future.successful(acc ++ currentPageItems)
        else
          dynamoSummingPager(queryRequestBuilder(Some(result.getLastEvaluatedKey)), acc ++ currentPageItems)
      }
    }

    val batchKeys = partitionKeys.partitionEventNums.map(s => messageKey(persistenceId, s) -> (s / PartitionSize))
    val batchKeysMap = batchKeys.iterator.map(p => p._1.get(Key) -> p._2).toMap
    dynamoSummingPager(queryRequestBuilder(None), Seq.empty).map(result => ReplayBatch(result, batchKeysMap))
  }

  def listAllSeqNr(persistenceId: String): Future[Seq[Long]] =
    Source
      .fromIterator { () => Iterator.iterate(0L)(_ + 1) }
      .via(DynamoPartitionGrouped)
      .mapAsync(ReplayParallelism)(batch => getPartitionItems(persistenceId, batch).map(_.ids.sorted))
      .takeWhile(_.nonEmpty)
      .runFold(Vector.empty[Long])(_ ++ _)

  def readSequenceNr(persistenceId: String, highest: Boolean): Future[Long] = {
    if (Tracing) log.debug("readSequenceNr(highest={}, persistenceId={})", highest, persistenceId)
    val keyGroups = readSequenceNrBatches(persistenceId, highest).map(_.map(getMaxSeqNr).recover {
      case ex: Throwable => -1L
    })
    Future.sequence(keyGroups).flatMap { seq =>
      seq.max match {
        case -1L =>
          val highOrLow = if (highest) "highest" else "lowest"
          throw new DynamoDBJournalFailure(s"cannot read $highOrLow sequence number for persistenceId $persistenceId")
        case start =>
          if (highest) {
            /*
             * When reading the highest sequence number the stored value always points to the Sort=0 entry
             * for which it was written, all other entries do not update the highest value. Therefore we
             * must scan the partition of this Sort=0 entry and find the highest occupied number.
             */
            getAllPartitionSequenceNrs(persistenceId, start).flatMap { result =>
              if (result.getItems.isEmpty) {
                /*
                 * If this comes back empty then that means that all events have been deleted. The only
                 * reliable way to obtain the previously highest number is to also read the lowest number
                 * (which is always stored in full), knowing that it will be either highest-1 or zero.
                 */
                readSequenceNr(persistenceId, highest = false).map { lowest =>
                  val ret = Math.max(start, lowest - 1)
                  log.debug("readSequenceNr(highest=true persistenceId={}) = {}", persistenceId, ret)
                  ret
                }
              } else if (Fixes.HighDistrust) { // allows recovering from failed high mark setting
                // this function will keep on chasing the event source tail
                // if HighDistrust is enabled and as long as the partitionMax == PartitionSize - 1
                def tailChase(partitionStart: Long, nextResults: QueryResult): Future[Long] = {
                  if (nextResults.getItems.isEmpty) {
                    // first iteraton will not pass here, as the query result is not empty
                    // if the new query result is empty the highest observed is partition -1
                    Future.successful(partitionStart - 1)
                  } else {
                    /*
                     * `partitionStart` is the Sort=0 entry’s sequence number, so add the maximum sort key.
                     */
                    val partitionMax = nextResults.getItems.asScala.map(_.get(Sort).getN.toLong).max
                    val ret = partitionStart + partitionMax

                    if (partitionMax == PartitionSize - 1) {
                      val nextStart = ret + 1
                      getAllPartitionSequenceNrs(persistenceId, nextStart)
                        .map { logResult =>
                          if (!logResult.getItems().isEmpty()) // will only log if a follow-up query produced results
                            log.warning(
                              "readSequenceNr(highest=true persistenceId={}) tail found after {}",
                              persistenceId,
                              ret)
                          logResult
                        }
                        .flatMap(tailChase(nextStart, _))
                    } else
                      Future.successful(ret)
                  }
                }

                tailChase(start, result).map { ret =>
                  log.debug("readSequenceNr(highest=true persistenceId={}) = {}", persistenceId, ret)
                  ret
                }
              } else {
                /*
                 * `start` is the Sort=0 entry’s sequence number, so add the maximum sort key.
                 */
                val ret = start + result.getItems.asScala.map(_.get(Sort).getN.toLong).max
                log.debug("readSequenceNr(highest=true persistenceId={}) = {}", persistenceId, ret)
                Future.successful(ret)
              }
            }
          } else {
            log.debug("readSequenceNr(highest=false persistenceId={}) = {}", persistenceId, start)
            Future.successful(start)
          }
      }
    }
  }

  def readAllSequenceNr(persistenceId: String, highest: Boolean): Future[Set[Long]] =
    Future
      .sequence(readSequenceNrBatches(persistenceId, highest).map(_.map(getAllSeqNr).recover {
        case ex: Throwable => Nil
      }))
      .map(_.flatten.toSet)

  def readSequenceNrBatches(persistenceId: String, highest: Boolean): Iterator[Future[BatchGetItemResult]] =
    (0 until SequenceShards).iterator
      .map(l => if (highest) highSeqKey(persistenceId, l) else lowSeqKey(persistenceId, l))
      .grouped(MaxBatchGet)
      .map { keys =>
        val ka = new KeysAndAttributes().withKeys(keys.asJava).withConsistentRead(true)
        val get = batchGetReq(Collections.singletonMap(JournalTable, ka))
        dynamo.batchGetItem(get).flatMap(getUnprocessedItems(_))
      }

  private def getMaxSeqNr(resp: BatchGetItemResult): Long =
    if (resp.getResponses.isEmpty) 0L
    else {
      var ret = 0L
      resp.getResponses
        .get(JournalTable)
        .forEach(new Consumer[Item] {
          override def accept(item: Item): Unit = {
            val seq = item.get(SequenceNr) match {
              case null => 0L
              case attr => attr.getN.toLong
            }
            if (seq > ret) ret = seq
          }
        })
      ret
    }

  private def getAllSeqNr(resp: BatchGetItemResult): Seq[Long] =
    if (resp.getResponses.isEmpty()) Nil
    else {
      var ret: List[Long] = Nil
      resp.getResponses
        .get(JournalTable)
        .forEach(new Consumer[Item] {
          override def accept(item: Item): Unit = {
            item.get(SequenceNr) match {
              case null =>
              case attr => ret ::= attr.getN.toLong
            }
          }
        })
      ret
    }

  private def getValueOrEmptyString(item: JMap[String, AttributeValue], key: String): String = {
    if (item.containsKey(key)) item.get(key).getS else ""
  }

  def readPersistentRepr(item: JMap[String, AttributeValue], async: Boolean): Future[PersistentRepr] = {
    val clazz = classOf[PersistentRepr]

    if (item.containsKey(Event)) {
      val serializerManifest = getValueOrEmptyString(item, SerializerManifest)

      val pI = item.get(PersistentId).getS
      val sN = item.get(SequenceNr).getN.toLong
      val wU = item.get(WriterUuid).getS
      val reprManifest = getValueOrEmptyString(item, Manifest)

      val eventPayload = item.get(Event).getB
      val serId = item.get(SerializerId).getN.toInt

      val fut = serialization.serializerByIdentity.get(serId) match {
        case Some(asyncSerializer: AsyncSerializer) =>
          Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
            asyncSerializer.fromBinaryAsync(eventPayload.array(), serializerManifest)
          }
        case _ =>
          def deserializedEvent: AnyRef = {
            // Serialization.deserialize adds transport info
            serialization.deserialize(eventPayload.array(), serId, serializerManifest).get
          }
          if (async) Future(deserializedEvent)
          else
            Future.successful(deserializedEvent)
      }

      fut.map { (event: AnyRef) =>
        PersistentRepr(
          event,
          sequenceNr = sN,
          persistenceId = pI,
          manifest = reprManifest,
          writerUuid = wU,
          sender = null)
      }

    } else {

      def deserializedEvent: PersistentRepr = {
        // Serialization.deserialize adds transport info
        serialization.deserialize(item.get(Payload).getB.array(), clazz).get
      }

      if (async) Future(deserializedEvent)
      else
        Future.successful(deserializedEvent)

    }
  }

  def getUnprocessedItems(result: BatchGetItemResult, retriesRemaining: Int = 10): Future[BatchGetItemResult] = {
    val unprocessed = result.getUnprocessedKeys.get(JournalTable) match {
      case null => 0
      case x    => x.getKeys.size
    }
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      Future.failed(
        new DynamoDBJournalFailure(
          s"unable to batch get ${result.getUnprocessedKeys.get(JournalTable).getKeys} after 10 tries"))
    } else {
      val rest = batchGetReq(result.getUnprocessedKeys)
      dynamo
        .batchGetItem(rest)
        .map { rr =>
          val items = rr.getResponses.get(JournalTable)
          val responses = result.getResponses.get(JournalTable)
          items.forEach(new Consumer[Item] {
            override def accept(item: Item): Unit = responses.add(item)
          })
          result.setUnprocessedKeys(rr.getUnprocessedKeys)
          result
        }
        .flatMap(getUnprocessedItems(_, retriesRemaining - 1))
    }
  }

  private[dynamodb] def getAllRemainingQueryItems(request: QueryRequest, result: QueryResult): Future[QueryResult] = {
    val last = result.getLastEvaluatedKey
    if (last == null || last.isEmpty || last.get(Sort).getN.toLong == 99) Future.successful(result)
    else {
      dynamo.query(request.withExclusiveStartKey(last)).flatMap { next =>
        val merged = new ArrayList[Item](result.getItems.size + next.getItems.size)
        merged.addAll(result.getItems)
        merged.addAll(next.getItems)

        // need to keep on reading until there's nothing more to read
        getAllRemainingQueryItems(request, next.withItems(merged))
      }
    }
  }

  def eventQuery(persistenceId: String, sequenceNr: Long) =
    new QueryRequest()
      .withTableName(JournalTable)
      .withKeyConditionExpression(Key + " = :kkey")
      .withExpressionAttributeValues(
        Collections.singletonMap(":kkey", S(messagePartitionKey(persistenceId, sequenceNr))))
      .withProjectionExpression("num")
      .withConsistentRead(true)

  private[dynamodb] def getAllPartitionSequenceNrs(persistenceId: String, sequenceNr: Long) = {
    val request = eventQuery(persistenceId, sequenceNr)
    dynamo.query(request).flatMap(getAllRemainingQueryItems(request, _))
  }

  def batchGetReq(items: JMap[String, KeysAndAttributes]) =
    new BatchGetItemRequest().withRequestItems(items).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def logFailure[T](desc: String)(f: Future[T]): Future[T] =
    f.transform(
      identity(_),
      ex => {
        log.error(ex, "operation failed: " + desc)
        ex
      })
}
