/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import java.util.function.Consumer
import akka.stream.scaladsl._
import scala.collection.immutable
import akka.actor.ActorContext
import akka.stream.ActorMaterializer

object DynamoDBRecovery {
  case class ReplayBatch(items: Seq[Item], map: Map[AttributeValue, Long]) {
    def sorted: immutable.Iterable[Item] =
      items.foldLeft(immutable.TreeMap.empty[Long, Item])((acc, i) =>
        acc.updated(map(i.get(Key)), i))
        .map(_._2)
  }
}

trait DynamoDBRecovery extends AsyncRecovery { this: DynamoDBJournal =>
  import DynamoDBRecovery._

  import settings._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(ReplayDispatcher)

  override def asyncReplayMessages(persistenceId: String,
                                   fromSequenceNr: Long,
                                   toSequenceNr: Long,
                                   max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logFailure("replay") {
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
      case Left(ex)      => throw ex
      case Right(result) => ReplayBatch(result.getResponses.get(JournalTable).asScala, batchKeys)
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    logFailure("read-highest")(readSequenceNr(persistenceId, highest = true))

  def readSequenceNr(persistenceId: String, highest: Boolean): Future[Long] = {
    log.debug("readSequenceNr(highest={}) persistenceId={}", highest, persistenceId)
    val default = if (highest) 0L else Long.MaxValue - 1
    val failure = if (highest) -1L else Long.MaxValue
    Future.sequence {
      (0 until SequenceShards).iterator
        .map(l => if (highest) highSeqKey(persistenceId, l) else lowSeqKey(persistenceId, l))
        .grouped(MaxBatchGet)
        .map {
          keys =>
            val keyColl = keys.map(k => fields(Key -> k)).asJava
            val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
            val get = batchGetReq(Collections.singletonMap(JournalTable, ka))
            dynamo.batchGetItem(get).map {
              case Left(ex) => failure
              case Right(resp) =>
                if (resp.getResponses.isEmpty) default
                else {
                  var ret = default
                  resp.getResponses.get(JournalTable).forEach(new Consumer[Item] {
                    override def accept(item: Item): Unit = {
                      val seq = item.get(SequenceNr) match {
                        case null => default
                        case attr => attr.getN.toLong
                      }
                      if (seq < ret ^ highest) ret = seq
                    }
                  })
                  ret
                }
            }.recover {
              case ex: Throwable =>
                log.error(ex, "unexpected failure condition in asyncReadHighestSequenceNr")
                failure
            }
        }
        .toStream
    }.map { seq =>
      val ret =
        if (highest) seq.max
        else {
          val min = seq.min
          if (min == default) 0L else min
        }
      if (ret == failure)
        throw new DynamoDBJournalFailure(s"cannot read ${if(highest)"highest"else"lowest"} sequence number for persistenceId $persistenceId")
      else {
        log.debug("readSequenceNr result for {}: {}", persistenceId, ret)
        ret
      }
    }
  }

  def readPersistentRepr(item: JMap[String, AttributeValue]): PersistentRepr =
    persistentFromByteBuffer(item.get(Payload).getB)

  def getUnprocessedItems(resultTry: AWSTry[BatchGetItemResult], retriesRemaining: Int = 10): Future[AWSTry[BatchGetItemResult]] =
    resultTry match {
      case Left(ex) => Future.successful(resultTry)
      case Right(result) =>
        val unprocessed = Option(result.getUnprocessedKeys.get(JournalTable)).map(_.getKeys.size()).getOrElse(0)
        if (unprocessed == 0) Future.successful(resultTry)
        else if (retriesRemaining == 0) {
          throw new DynamoDBJournalFailure(s"unable to batch get $result after 10 tries")
        } else {
          val rest = batchGetReq(result.getUnprocessedKeys)
          dynamo.batchGetItem(rest).map {
            case l @ Left(_) => l
            case Right(rr) =>
              val items = rr.getResponses.get(JournalTable)
              val responses = result.getResponses.get(JournalTable)
              items.forEach(new Consumer[Item] {
                override def accept(item: Item): Unit = responses.add(item)
              })
              result.setUnprocessedKeys(rr.getUnprocessedKeys)
              resultTry
          }.flatMap(getUnprocessedItems(_, retriesRemaining - 1))
        }
    }

  def batchGetReq(items: JMap[String, KeysAndAttributes]) =
    new BatchGetItemRequest()
      .withRequestItems(items)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
}
