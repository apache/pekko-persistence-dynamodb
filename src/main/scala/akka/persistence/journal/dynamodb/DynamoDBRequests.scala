package akka.persistence.journal.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import DynamoDBJournal._
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import scala.collection.{mutable, immutable}
import java.util.{HashMap => JHMap, Map => JMap}
import collection.JavaConverters._
import scala.concurrent.Future
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult
import com.amazonaws.services.dynamodbv2.model.PutItemRequest


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
        val reqItems = fields(journalName -> writes.asJava)
        new BatchWriteItemRequest().withRequestItems(reqItems)
    }

    writes.map {
      write =>
        dynamo.sendBatchWriteItem(write).flatMap(sendUnprocessedItems).map {
          _ => if (log.isDebugEnabled) {
            log.debug("at=batch-write-finish writes={}", write.getRequestItems.get(journalName).size())
          } else ()
        }
    }

  }

  private [dynamodb] def sendUnprocessedItems(result: BatchWriteItemResult): Future[BatchWriteItemResult] = {
    val unprocessed: Int = result.getUnprocessedItems.size()
    if (unprocessed == 0) Future.successful(result)
    else {
      log.warning("at=unprocessed-items unprocessed={}", unprocessed)
      Future.sequence {
        result.getUnprocessedItems.get(journalName).asScala.map {
          w =>
            val p = new PutItemRequest().withTableName(journalName).withItem(w.getPutRequest.getItem)
            sendUnprocessedItem(p)
        }
      }.map {
        results =>
          result.getUnprocessedItems.clear()
          result //just return the original result
      }
    }
  }

  private [dynamodb] def sendUnprocessedItem(p: PutItemRequest, retries: Int = 5): Future[PutItemResult] = {
    //todo exponential backoff?
    if (retries == 0)  Future.failed(new RuntimeException(s"couldnt put ${p.getItem.get(Key)} after 5 tries"))
    else dynamo.sendPutItem(p).fallbackTo(sendUnprocessedItem(p, retries - 1))
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
            val put = new PutItemRequest().withTableName(journalName).withItem(item)
            dynamo.sendPutItem(put).map(_ => log.debug("at=update-sequence-low-shard processorId={} sequenceId={}", msg.processorId, msg.sequenceNr))
        }
    }
  }




  def toMsgItem(repr:PersistentRepr):Item = fields(
    Key -> messageKey(repr.processorId, repr.sequenceNr),
    Payload -> B(serialization.serialize(repr).get),
    Deleted -> S(false)
  )

  def toHSItem(repr:PersistentRepr):Item = fields(
    Key -> highSeqKey(repr.processorId, repr.sequenceNr % sequenceShards),
    SequenceNr -> N(repr.sequenceNr)
  )

  def toLSItem(id:PersistentId):Item = fields(
    Key -> lowSeqKey(id.processorId, id.sequenceNr % sequenceShards),
    SequenceNr -> N(id.sequenceNr)
  )

  def put(item:Item):WriteRequest = new WriteRequest().withPutRequest(new PutRequest().withItem(item))

  def delete(item:Item):WriteRequest = new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(item))

  def updateItem(key:Item, updates:ItemUpdates):UpdateItemRequest = new UpdateItemRequest().withTableName(journalName).withKey(key).withAttributeUpdates(updates)

  def setAdd(value:AttributeValue):AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(value)

  def permanentDeleteToDelete(id: PersistentId): DeleteItemRequest = {
    log.debug("delete permanent {}", id)
    val key = fields(Key -> messageKey(id.processorId, id.sequenceNr))
    new DeleteItemRequest().withTableName(journalName).withKey(key)
  }

  def impermanentDeleteToUpdate(id: PersistentId): UpdateItemRequest = {
    log.debug("delete {}", id)
    val key = fields(Key -> messageKey(id.processorId, id.sequenceNr))
    val updates = fields(Deleted -> new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(S(true)))
    new UpdateItemRequest().withTableName(journalName).withKey(key).withAttributeUpdates(updates)
  }

  def unitSequence(seq:TraversableOnce[Future[Unit]]):Future[Unit] = Future.sequence(seq).map(_ => ())

}
