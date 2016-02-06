/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }
import java.util.function.Consumer
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{ Failure, Success }
import akka.actor.ActorContext
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

object DynamoDBRecovery {
  case class ReplayBatch(items: Seq[Item], map: Map[AttributeValue, Long]) {
    def sorted: immutable.Iterable[Item] =
      items.foldLeft(immutable.TreeMap.empty[Long, Item])((acc, i) =>
        acc.updated(map(i.get(Key)), i))
        .map(_._2)
    def ids: Seq[Long] = items.map(i => map(i.get(Key))).sorted
  }
}

trait DynamoDBRecovery extends AsyncRecovery { this: DynamoDBJournal =>
  import DynamoDBRecovery._

  import settings._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(ReplayDispatcher)

  override def asyncReplayMessages(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    max: Long
  )(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logFailure("replay") {
    log.debug("starting replay for {} from {} to {} (max {})", persistenceId, fromSequenceNr, toSequenceNr, max)
    // toSequenceNr is already capped to highest and guaranteed to be no less than fromSequenceNr
    readSequenceNr(persistenceId, highest = false).flatMap { lowest =>
      val start = Math.max(fromSequenceNr, lowest)
      Source(start to toSequenceNr)
        .grouped(MaxBatchGet)
        .mapAsync(ReplayParallelism)(batch => getReplayBatch(persistenceId, batch).map(_.sorted))
        .mapConcat(identity)
        .take(max)
        .map(readPersistentRepr)
        .runFold(0) { (count, next) => replayCallback(next); count + 1 }
        .map(count => log.debug("replay finished for {} with {} events", persistenceId, count))
    }
  }

  def getReplayBatch(persistenceId: String, seqNrs: Seq[Long]): Future[ReplayBatch] = {
    val batchKeys: Map[AttributeValue, Long] = seqNrs.map(s => messageKey(persistenceId, s) -> s)(collection.breakOut)
    val keyAttr = new KeysAndAttributes()
      .withKeys(batchKeys.keysIterator.map(Collections.singletonMap(Key, _)).toVector.asJava)
      .withConsistentRead(true)
      .withAttributesToGet(Key, Payload)
    val get = batchGetReq(Collections.singletonMap(JournalTable, keyAttr))
    dynamo.batchGetItem(get).flatMap(getUnprocessedItems(_)).map {
      case result => ReplayBatch(result.getResponses.get(JournalTable).asScala, batchKeys)
    }
  }

  def listAllSeqNr(persistenceId: String): Future[Seq[Long]] =
    Source.fromIterator { () => Iterator.iterate(0L)(_ + 1) }
      .grouped(MaxBatchGet)
      .mapAsync(ReplayParallelism)(batch => getReplayBatch(persistenceId, batch).map(_.ids))
      .takeWhile(_.nonEmpty)
      .runFold(Vector.empty[Long])(_ ++ _)

  def readSequenceNr(persistenceId: String, highest: Boolean): Future[Long] = {
    log.debug("readSequenceNr(highest={}) persistenceId={}", highest, persistenceId)
    val keyGroups = readSequenceNrBatches(persistenceId, highest)
      .map(_.map(getMaxSeqNr).recover {
        case ex: Throwable =>
          log.error(ex, "unexpected failure condition in asyncReadHighestSequenceNr")
          -1L
      })
    Future.sequence(keyGroups).map { seq =>
      seq.max match {
        case -1L =>
          val highOrLow = if (highest) "highest" else "lowest"
          throw new DynamoDBJournalFailure(s"cannot read $highOrLow sequence number for persistenceId $persistenceId")
        case ret =>
          log.debug("readSequenceNr result for {}: {}", persistenceId, ret)
          ret
      }
    }
  }

  private def getMaxSeqNr(resp: BatchGetItemResult): Long =
    if (resp.getResponses.isEmpty) 0L
    else {
      var ret = 0L
      resp.getResponses.get(JournalTable).forEach(new Consumer[Item] {
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
      resp.getResponses.get(JournalTable).forEach(new Consumer[Item] {
        override def accept(item: Item): Unit = {
          item.get(SequenceNr) match {
            case null =>
            case attr => ret ::= attr.getN.toLong
          }
        }
      })
      ret
    }

  def readAllSequenceNr(persistenceId: String, highest: Boolean): Future[Set[Long]] =
    Future.sequence(readSequenceNrBatches(persistenceId, highest)
      .map(_.map(getAllSeqNr).recover { case ex: Throwable => Nil }))
      .map(_.flatten.toSet)

  def readSequenceNrBatches(persistenceId: String, highest: Boolean): Iterator[Future[BatchGetItemResult]] =
    (0 until SequenceShards)
      .iterator
      .map(l => if (highest) highSeqKey(persistenceId, l) else lowSeqKey(persistenceId, l))
      .grouped(MaxBatchGet)
      .map { keys =>
        val keyColl = keys.map(k => Collections.singletonMap(Key, k)).asJava
        val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
        val get = batchGetReq(Collections.singletonMap(JournalTable, ka))
        dynamo.batchGetItem(get).flatMap(getUnprocessedItems(_))
      }

  def readPersistentRepr(item: JMap[String, AttributeValue]): PersistentRepr =
    persistentFromByteBuffer(item.get(Payload).getB)

  def getUnprocessedItems(result: BatchGetItemResult, retriesRemaining: Int = 10): Future[BatchGetItemResult] = {
    val unprocessed = Option(result.getUnprocessedKeys.get(JournalTable)).map(_.getKeys.size()).getOrElse(0)
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      Future.failed(new DynamoDBJournalFailure(s"unable to batch get $result after 10 tries"))
    } else {
      val rest = batchGetReq(result.getUnprocessedKeys)
      dynamo.batchGetItem(rest).map { rr =>
        val items = rr.getResponses.get(JournalTable)
        val responses = result.getResponses.get(JournalTable)
        items.forEach(new Consumer[Item] {
          override def accept(item: Item): Unit = responses.add(item)
        })
        result.setUnprocessedKeys(rr.getUnprocessedKeys)
        result
      }.flatMap(getUnprocessedItems(_, retriesRemaining - 1))
    }
  }

  def batchGetReq(items: JMap[String, KeysAndAttributes]) =
    new BatchGetItemRequest()
      .withRequestItems(items)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
}
