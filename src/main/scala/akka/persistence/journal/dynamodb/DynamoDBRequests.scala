package akka.persistence.journal.dynamodb

import DynamoDBJournal._
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.{mutable, immutable}
import scala.concurrent.Future


trait DynamoDBRequests {
  this: DynamoDBJournal =>

  def writeMessages(messages: immutable.Seq[PersistentRepr]): Future[Unit] = unitSequence {
    // 25 is max items so group by 12 since 2 items per message
    // todo size calculation < 1M
    val writes = messages.grouped(12).map {
      msgs =>
        val writes = msgs.foldLeft(new mutable.ArrayBuffer[WriteRequest](messages.length)) {
          case (ws, repr) =>
            ws += put(toMsgItem(repr))
            ws += put(toHSItem(repr))
            ws
        }
        val reqItems = fields(journalTable -> writes.asJava)
        new BatchWriteItemRequest().withRequestItems(reqItems)
    }

    writes.map {
      write =>
        batchWrite(write).flatMap(r => sendUnprocessedItems(r)).map {
          _ => if (log.isDebugEnabled) {
            log.debug("at=batch-write-finish writes={}", write.getRequestItems.get(journalTable).size())
          } else ()
        }
    }

  }

  private[dynamodb] def sendUnprocessedItems(result: BatchWriteItemResult, retriesRemaining:Int=10): Future[BatchWriteItemResult] = {
    val unprocessed: Int = result.getUnprocessedItems.size()
    if (unprocessed == 0) Future.successful(result)
    else if(retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write ${result} after 10 tries")
    } else {
      log.warning("at=unprocessed-writes unprocessed={}", unprocessed)
      backoff(10 - retriesRemaining)
      val rest = new BatchWriteItemRequest().withRequestItems(result.getUnprocessedItems)
      batchWrite(rest, retriesRemaining - 1).flatMap(r => sendUnprocessedItems(r, retriesRemaining - 1))
    }
  }

  def batchWrite(r:BatchWriteItemRequest, retriesRemaining:Int=10):Future[BatchWriteItemResult]={
    dynamo.batchWriteItem(r).flatMap{
      case Left(t:ProvisionedThroughputExceededException) =>
        backoff(retriesRemaining)
        batchWrite(r,retriesRemaining-1)
      case Left(e) =>
        throw e
      case Right(resp) =>
        Future.successful(resp)
    }
  }



  def writeConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = unitSequence {
    confirmations.groupBy(c => (c.processorId, c.sequenceNr)).map {
      case ((processorId, sequenceNr), confirms) =>
        val key = fields(Key -> messageKey(processorId, sequenceNr))
        val update = fields(Confirmations -> setAdd(SS(confirmations.map(_.channelId))))
        dynamo.sendUpdateItem(updateItem(key, update)).map {
          result => log.debug("at=confirmed key={} update={}", key, update)
        }
    }
  }

  def deleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = unitSequence {
    messageIds.map {
      msg =>
        if (permanent) {
          dynamo.sendDeleteItem(permanentDeleteToDelete(msg)).map {
            _ => log.debug("at=permanent-delete-item  processorId={} sequenceId={}", msg.processorId, msg.sequenceNr)
          }
        } else {
          dynamo.sendUpdateItem(impermanentDeleteToUpdate(msg)).map {
            _ => log.debug("at=mark-delete-item  processorId={} sequenceId={}", msg.processorId, msg.sequenceNr)
          }
        }.flatMap {
          _ =>
            val item = toLSItem(msg)
            val put = new PutItemRequest().withTableName(journalTable).withItem(item)
            dynamo.sendPutItem(put).map(_ => log.debug("at=update-sequence-low-shard processorId={} sequenceId={}", msg.processorId, msg.sequenceNr))
        }
    }
  }


  def toMsgItem(repr: PersistentRepr): Item = fields(
    Key -> messageKey(repr.processorId, repr.sequenceNr),
    Payload -> B(serialization.serialize(repr).get),
    Deleted -> S(false)
  )

  def toHSItem(repr: PersistentRepr): Item = fields(
    Key -> highSeqKey(repr.processorId, repr.sequenceNr % sequenceShards),
    SequenceNr -> N(repr.sequenceNr)
  )

  def toLSItem(id: PersistentId): Item = fields(
    Key -> lowSeqKey(id.processorId, id.sequenceNr % sequenceShards),
    SequenceNr -> N(id.sequenceNr)
  )

  def put(item: Item): WriteRequest = new WriteRequest().withPutRequest(new PutRequest().withItem(item))

  def delete(item: Item): WriteRequest = new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(item))

  def updateItem(key: Item, updates: ItemUpdates): UpdateItemRequest = new UpdateItemRequest().withTableName(journalTable).withKey(key).withAttributeUpdates(updates)

  def setAdd(value: AttributeValue): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(value)

  def permanentDeleteToDelete(id: PersistentId): DeleteItemRequest = {
    log.debug("delete permanent {}", id)
    val key = fields(Key -> messageKey(id.processorId, id.sequenceNr))
    new DeleteItemRequest().withTableName(journalTable).withKey(key)
  }

  def impermanentDeleteToUpdate(id: PersistentId): UpdateItemRequest = {
    log.debug("delete {}", id)
    val key = fields(Key -> messageKey(id.processorId, id.sequenceNr))
    val updates = fields(Deleted -> new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(S(true)))
    new UpdateItemRequest().withTableName(journalTable).withKey(key).withAttributeUpdates(updates)
  }

  def unitSequence(seq: TraversableOnce[Future[Unit]]): Future[Unit] = Future.sequence(seq).map(_ => ())

}
