/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.{ Collections, Map => JMap }
import java.util.function.Consumer
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import com.amazonaws.services.dynamodbv2.model._

import scala.jdk.CollectionConverters._
import scala.collection.immutable
import scala.concurrent.Future
import akka.stream.scaladsl._
import java.util.ArrayList

import akka.actor.ExtendedActorSystem
import akka.stream.stage._
import akka.stream._
import akka.persistence.dynamodb._
import akka.serialization.{ AsyncSerializer, Serialization }

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
    def ids: Seq[Long]                   = items.map(itemToSeq).sorted
    private def itemToSeq(i: Item): Long = map(i.get(Key)) * 100 + i.get(Sort).getN.toInt
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
 *
 * @param n - the size of partitions, this is hardcoded at 100 in other places in this library
 */
case class DynamoPartitionGrouped(n: Int) extends GraphStage[FlowShape[Long, PartitionKeys]] {
  require(n > 0, "n must be greater than 0")

  val in  = Inlet[Long]("DynamoEventNum.in")
  val out = Outlet[PartitionKeys]("DynamoPartitionKeys.out")

  override val initialAttributes = Attributes.name("DynamoPartitionGrouped")

  override val shape: FlowShape[Long, PartitionKeys] = FlowShape(in, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val partitionBuf = {
        val b = Vector.newBuilder[Long]
        b.sizeHint(n)
        b
      }
      var hasElements = false

      def pushOut(currentSeqNo: Long, partitionGroup: Vector[Long]): Unit = {
        partitionBuf.clear()
        hasElements = false
        val partitionSeqNo = currentSeqNo / n
        push(out, PartitionKeys(partitionSeqNo, partitionGroup))
      }

      override def onPush(): Unit = {
        val currentSeqNo = grab(in)
        partitionBuf += currentSeqNo
        hasElements = true

        //If the next entry received would result in the next partition, then we clear the buf and push the results out
        if ((currentSeqNo + 1) % n == 0) {
          val partitionGroup = partitionBuf.result()
          pushOut(currentSeqNo, partitionGroup)
        } else {
          pull(in)
        }
      }

      override def onPull(): Unit = pull(in)

      override def onUpstreamFinish(): Unit = {
        //this means the partitionBuf has elements but not a full amount (n). However, since upstream is finished
        //publishing elements, we need to push what we have downstream.
        if (hasElements) {
          val partitionGroup = partitionBuf.result()
          val currentSeqNo   = partitionGroup.last
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

  val in  = Inlet[Item]("RIA.in")
  val out = Outlet[List[Item]]("RIA.out")

  override val shape             = FlowShape(in, out)
  override val initialAttributes = Attributes.name("RemoveIncompleteAtoms")

  override def createLogic(attr: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      var batchEnd = NoBatch
      var batch    = List.empty[Item]

      setHandler(out, this)
      setHandler(in, this)

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = {
        val item = grab(in)
        if (item.containsKey(AtomEnd)) {
          val end        = item.get(AtomEnd).getN.toLong
          val index      = item.get(AtomIndex).getN.toLong
          val seqNr      = sn(item)
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
        val s   = item.get(Key).getS
        val n   = item.get(Sort).getN.toLong
        val pos = s.lastIndexOf('-')
        require(pos != -1, "unknown key format " + s)
        s.substring(pos + 1).toLong * 100 + n
      }

    }
}

trait DynamoDBRecovery extends AsyncRecovery { this: DynamoDBJournal =>
  import DynamoDBRecovery._
  import settings._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(ReplayDispatcher)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    logFailure(s"replay for $persistenceId ($fromSequenceNr to $toSequenceNr)") {
      log.debug("starting replay for {} from {} to {} (max {})", persistenceId, fromSequenceNr, toSequenceNr, max)
      // toSequenceNr is already capped to highest and guaranteed to be no less than fromSequenceNr
      readSequenceNr(persistenceId, highest = false).flatMap { lowest =>
        val start = Math.max(fromSequenceNr, lowest)
        val async = ReplayParallelism > 1
        Source(start to toSequenceNr)
          .via(DynamoPartitionGrouped(100))
          .mapAsync(ReplayParallelism)(batch => getPartitionItems(persistenceId, batch).map(_.sorted))
          .mapConcat(identity)
          .take(max)
          .via(RemoveIncompleteAtoms)
          .mapConcat(identity)
          .mapAsync(ReplayParallelism)(readPersistentRepr(_, async))
          .runFold(0) { (count, next) => replayCallback(next); count + 1 }
          .map(count => log.debug("replay finished for {} with {} events", persistenceId, count))
      }
    }

  def getPartitionItems(persistenceId: String, partitionKeys: PartitionKeys): Future[ReplayBatch] = {
    val sortedNrs    = partitionKeys.partitionEventNums.sorted.map(_ % 100)
    val startSortKey = sortedNrs.head
    val endSortKey   = sortedNrs.last

    val queryRequestBuilder: (Option[java.util.Map[String, AttributeValue]]) => QueryRequest = exclusiveStartKeyOpt => {
      val request = new QueryRequest()
        .withTableName(JournalTable)
        .withKeyConditionExpression(s"$Key = :kkey AND $Sort BETWEEN :startSKey AND :endSKey")
        .withExpressionAttributeValues(
          Map(
            ":kkey"      -> S(messagePartitionKeyFromGroupNr(persistenceId, partitionKeys.partitionSeqNum)),
            ":startSKey" -> N(startSortKey),
            ":endSKey"   -> N(endSortKey)).asJava)
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

    val batchKeys    = partitionKeys.partitionEventNums.map(s => messageKey(persistenceId, s) -> (s / 100))
    val batchKeysMap = batchKeys.iterator.map(p => p._1.get(Key) -> p._2).toMap
    dynamoSummingPager(queryRequestBuilder(None), Seq.empty).map(result => ReplayBatch(result, batchKeysMap))
  }

  def listAllSeqNr(persistenceId: String): Future[Seq[Long]] =
    Source
      .fromIterator { () => Iterator.iterate(0L)(_ + 1) }
      .via(DynamoPartitionGrouped(100))
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
            val request = eventQuery(persistenceId, start)
            dynamo.query(request).flatMap(getRemainingQueryItems(request, _)).flatMap { result =>
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
        val ka  = new KeysAndAttributes().withKeys(keys.asJava).withConsistentRead(true)
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

      val pI           = item.get(PersistentId).getS
      val sN           = item.get(SequenceNr).getN.toLong
      val wU           = item.get(WriterUuid).getS
      val reprManifest = getValueOrEmptyString(item, Manifest)

      val eventPayload = item.get(Event).getB
      val serId        = item.get(SerializerId).getN.toInt

      val fut = serialization.serializerByIdentity.get(serId) match {
        case Some(asyncSerializer: AsyncSerializer) =>
          Serialization.withTransportInformation(context.system.asInstanceOf[ExtendedActorSystem]) { () =>
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

      fut.map { event: AnyRef =>
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
          val items     = rr.getResponses.get(JournalTable)
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

  def getRemainingQueryItems(request: QueryRequest, result: QueryResult): Future[QueryResult] = {
    val last = result.getLastEvaluatedKey
    if (last == null || last.isEmpty || last.get(Sort).getN.toLong == 99) Future.successful(result)
    else {
      dynamo.query(request.withExclusiveStartKey(last)).map { next =>
        val merged = new ArrayList[Item](result.getItems.size + next.getItems.size)
        merged.addAll(result.getItems)
        merged.addAll(next.getItems)
        next.withItems(merged)
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

  def batchGetReq(items: JMap[String, KeysAndAttributes]) =
    new BatchGetItemRequest().withRequestItems(items).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
}
