/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.{ Collections, List => JList, Map => JMap, HashMap => JHMap }
import akka.persistence.{ PersistentRepr, AtomicWrite }
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import scala.collection.{ immutable, mutable }
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal
import akka.Done

trait DynamoDBRequests {
  this: DynamoDBJournal =>

  import settings._

  /**
   * Writes all PersistentRepr in the AtomicWrite provided
   *
   * If there are any errors serializing (preparing the batch writes), then we must return
   * a Future.success(Failure) as the result.  This is needed to be compliant with
   * Akka Persistence 2.4
   *
   * @param atomicWrite Contains a list of persistentRepr that need to be persisted atomically
   * @return a successfully completed Future that contains either a Success or Failure
   */
  def writeMessages(atomicWrite: AtomicWrite): Future[Try[Unit]] = {

    // groups messages into groups of 12, a BatchWriteItemRequest has a max of 25 items, and
    // each PersistentRepr has two writes, the most we can put in any one request is 12 PersistentRepr
    val writes = Try { atomicWrite.payload.grouped(12).map(toBatchWriteItemRequest).toSeq }

    writes match {
      case Success(batchWrites) =>
        // we created our batch writes successfully, send them off to Dynamo
        val futures = writes.get.map {
          write =>

            // Dynamo can partially fail a number of write items in a batch write, usually because
            // of throughput constraints.  We need to continue to retry the unprocessed item
            // until we exhaust our backoff
            dynamo.batchWriteItem(write).flatMap(r => sendUnprocessedItems(r))
        }

        // Squash all of the futures into a single result
        trySequence(futures).map(seq => Try(seq.foreach(_.get)))

      case Failure(e) =>
        log.error(e, "DynamoDB Journal encountered error creating writes for {}: {}", atomicWrite.persistenceId, e.getMessage)

        val rej =
          if (atomicWrite.size == 1) e
          else new DynamoDBJournalRejection(s"AtomicWrite rejected as a whole due to ${e.getMessage}", e)

        // Note: In akka 2.4, asyncWriteMessages takes a Seq[AtomicWrite].  Akka assumes that you will
        // return a result for every item in the Sequence (the result is a Future[Seq[Try[Unit]]];
        // therefore, we should not explicitly fail any given future due to serialization, but rather
        // we should "Reject" it, meaning we return successfully with a Failure
        Future.successful(Failure(rej))
    }
  }

  def deleteMessages(persistenceId: String, start: Long, end: Long): Future[Done] =
    doBatch(
      batch => s"execute batch delete $batch",
      (start to end).map(deleteReq(persistenceId, _))
    )

  def setHS(persistenceId: String, to: Long): Future[PutItemResult] = {
    val item = toHSItem(persistenceId, to)
    val put = new PutItemRequest().withTableName(JournalTable).withItem(item)
    dynamo.putItem(put)
  }

  def removeHS(persistenceId: String): Future[Done] =
    doBatch(
      _ => s"remove HS entry for $persistenceId",
      (0 until SequenceShards).map(deleteHSItem(persistenceId, _))
    )

  def setLS(persistenceId: String, to: Long): Future[PutItemResult] = {
    val item = toLSItem(persistenceId, to)
    val put = new PutItemRequest().withTableName(JournalTable).withItem(item)
    dynamo.putItem(put)
  }

  def removeLS(persistenceId: String): Future[Done] =
    doBatch(
      _ => s"remove LS entry for $persistenceId",
      (0 until SequenceShards).map(deleteLSItem(persistenceId, _))
    )

  /*
   * Request and Item construction helpers.
   */

  /**
   * Converts a sequence of PersistentRepr to a single batch write request
   */
  private def toBatchWriteItemRequest(msgs: Seq[PersistentRepr]): BatchWriteItemRequest = {
    val writes = msgs.foldLeft(new mutable.ArrayBuffer[WriteRequest](msgs.length)) {
      case (ws, repr) =>
        ws += putReq(toMsgItem(repr))
        ws += putReq(toHSItem(repr.persistenceId, repr.sequenceNr))
        ws
    }
    val reqItems = Collections.singletonMap(JournalTable, writes.asJava)
    batchWriteReq(reqItems)
  }

  private def toMsgItem(repr: PersistentRepr): Item = {
    val k = messageKey(repr.persistenceId, repr.sequenceNr)
    val v = B(serialization.serialize(repr).get)
    val itemSize = KeyPayloadOverhead + k.getS.length + v.getB.remaining
    if (itemSize > MaxItemSize)
      throw new DynamoDBJournalRejection(s"MaxItemSize exceeded: $itemSize > $MaxItemSize")
    val item: Item = new JHMap(2, 1.0f)
    item.put(Key, k)
    item.put(Payload, v)
    item
  }

  private def toHSItem(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap(2, 1.0f)
    item.put(Key, highSeqKey(persistenceId, sequenceNr % SequenceShards))
    item.put(SequenceNr, N(sequenceNr))
    item
  }

  private def deleteHSItem(persistenceId: String, shard: Int): WriteRequest =
    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(
      Collections.singletonMap(Key, highSeqKey(persistenceId, shard))
    ))

  private def toLSItem(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap(2, 1.0f)
    item.put(Key, lowSeqKey(persistenceId, sequenceNr % SequenceShards))
    item.put(SequenceNr, N(sequenceNr))
    item
  }

  private def deleteLSItem(persistenceId: String, shard: Int): WriteRequest =
    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(
      Collections.singletonMap(Key, lowSeqKey(persistenceId, shard))
    ))

  private def putReq(item: Item): WriteRequest = new WriteRequest().withPutRequest(new PutRequest().withItem(item))

  private def deleteReq(persistenceId: String, sequenceNr: Long): WriteRequest =
    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(
      Collections.singletonMap(Key, messageKey(persistenceId, sequenceNr))
    ))

  private def batchWriteReq(writes: Seq[WriteRequest]): BatchWriteItemRequest =
    batchWriteReq(Collections.singletonMap(JournalTable, writes.asJava))

  private def batchWriteReq(items: JMap[String, JList[WriteRequest]]): BatchWriteItemRequest =
    new BatchWriteItemRequest()
      .withRequestItems(items)
      .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  /*
   * Request execution helpers.
   */

  /**
   * Execute the given WriteRequests in batches of MaxBatchWrite, ignoring and
   * logging all errors. The returned Future never fails.
   */
  private def doBatch(desc: Seq[WriteRequest] => String, writes: Seq[WriteRequest]): Future[Done] =
    Future.sequence {
      writes
        .grouped(MaxBatchWrite)
        .map { batch =>
          dynamo.batchWriteItem(batchWriteReq(batch))
            .flatMap(sendUnprocessedItems(_))
            .recover {
              case NonFatal(ex) => log.error(ex, "cannot " + desc(batch))
            }
        }
    }.map(_ => Done)

  /**
   * Sends the unprocessed batch write items, and sets the back-off.
   * if no more retries remain (number of back-off retries exhausted, we throw a Runtime exception
   *
   * Note: the dynamo db client supports automatic retries, however a batch will not fail if some of the items in the
   * batch fail; that is why we need our own back-off mechanism here.  If we exhaust OUR retry logic on top of
   * the retries from teh client, then we are hosed and cannot continue; that is why we have a RuntimeException here
   */
  private def sendUnprocessedItems(result: BatchWriteItemResult, retriesRemaining: Int = 10): Future[BatchWriteItemResult] = {
    val unprocessed: Int = Option(result.getUnprocessedItems.get(JournalTable)).map(_.size()).getOrElse(0)
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write $result after 10 tries")
    } else {
      val rest = batchWriteReq(result.getUnprocessedItems)
      dynamo.batchWriteItem(rest).flatMap(r => sendUnprocessedItems(r, retriesRemaining - 1))
    }
  }

}
