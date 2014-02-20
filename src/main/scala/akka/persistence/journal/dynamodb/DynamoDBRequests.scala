package akka.persistence.journal.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import DynamoDBJournal._
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import scala.collection.immutable
import java.util.{HashMap => JHMap, Map => JMap}


trait DynamoDBRequests {
  this: DynamoDBJournal =>

  def queryMessages(processorId: String, fromSequenceNr: Long): QueryRequest = {
    val q = new QueryRequest()
    val hashKeyCondition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(S(processorId))
    val rangeKeyCondition = new Condition()
      .withComparisonOperator(ComparisonOperator.GE)
      .withAttributeValueList(N(fromSequenceNr))
    val conditions = fields(ProcessorId -> hashKeyCondition, SequenceNr -> rangeKeyCondition)
    q.withConsistentRead(true)
      .withTableName(journalName)
      .withKeyConditions(conditions)
      .withSelect(Select.ALL_ATTRIBUTES).withScanIndexForward(true)
  }

  def readPersistentRepr(item: JMap[String, AttributeValue]): Option[PersistentRepr] = {
    import collection.JavaConverters._
    item.asScala.get(Payload).map {
      payload =>
        val repr = persistentFromByteBuffer(payload.getB)
        val isDeleted = item.get(Deleted).getS == "true"
        val confirmations = item.asScala.get(Confirmations).map {
          ca => ca.getSS().asScala.to[immutable.Seq]
        }.getOrElse(immutable.Seq[String]())
        repr.update(deleted = isDeleted, confirms = confirmations)
    }
  }

  def persistentToPut(repr: PersistentRepr): PutItemRequest = {
    val item = fields(ProcessorId -> S(repr.processorId), SequenceNr -> N(repr.sequenceNr),
      Payload -> B(serialization.serialize(repr).get), Deleted -> S(false))
    new PutItemRequest().withTableName(journalName).withItem(item)
  }


  def permanentDeleteToDelete(id: PersistentId): DeleteItemRequest = {
    log.debug("delete permanent {}", id)
    val key = fields(ProcessorId -> S(id.processorId), SequenceNr -> N(id.sequenceNr))
    new DeleteItemRequest().withTableName(journalName).withKey(key)
  }

  def impermanentDeleteToUpdate(id: PersistentId): UpdateItemRequest = {
    log.debug("delete {}", id)
    val key = fields(ProcessorId -> S(id.processorId), SequenceNr -> N(id.sequenceNr))
    val updates = fields(Deleted -> new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(S(true)))
    new UpdateItemRequest().withTableName(journalName).withKey(key).withAttributeUpdates(updates)
  }

  def querySequence(processorId: String, fromSequenceNr: Long, ascending: Boolean): QueryRequest = {
    val q = new QueryRequest()
    val hashKeyCondition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(S(processorId))
    val rangeKeyCondition = new Condition()
      .withComparisonOperator(if (ascending) ComparisonOperator.LE else ComparisonOperator.GE)
      .withAttributeValueList(N(fromSequenceNr))
    val conditions = fields(ProcessorId -> hashKeyCondition, SequenceNr -> rangeKeyCondition)
    q.withConsistentRead(true)
      .withTableName(journalName)
      .withKeyConditions(conditions)
      .withSelect(Select.ALL_ATTRIBUTES).withScanIndexForward(ascending).withLimit(1)
  }

  def confirmationToUpdate(confirmation: PersistentConfirmation): UpdateItemRequest = {
    val key = fields(ProcessorId -> S(confirmation.processorId), SequenceNr -> N(confirmation.sequenceNr))
    val updates = fields(Confirmations -> US(confirmation.channelId))
    new UpdateItemRequest().withTableName(journalName).withKey(key).withAttributeUpdates(updates)
  }



}
