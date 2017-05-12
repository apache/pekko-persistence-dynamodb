package akka.persistence.dynamodb.snapshot

import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }

import akka.Done
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.dynamodb._
import akka.persistence.dynamodb.journal.DynamoDBRequests
import akka.persistence.serialization.Snapshot
import com.amazonaws.services.dynamodbv2.model._
import akka.pattern.after
import collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.concurrent.duration._

trait DynamoDBSnapshotRequests extends DynamoDBRequests {
  this: DynamoDBSnapshotStore =>

  import settings._
  import context.dispatcher

  def delete(metadata: SnapshotMetadata): Future[Unit] = {
    val request = new DeleteItemRequest()
      .withTableName(Table)
      .addKeyEntry(Key, S(messagePartitionKey(metadata.persistenceId)))
      .addKeyEntry(SequenceNr, N(metadata.sequenceNr))

    dynamo.deleteItem(request)
      .map(_ => ())
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    for {
      result <- loadQueryResult(persistenceId, criteria).map(result => result.getItems.asScala.map(item => item.get(SequenceNr).getN.toLong))
      _ <- doBatch(
        batch => s"execute batch delete $batch",
        result.map(snapshotDeleteReq(persistenceId, _))
      )
    } yield ()
  }

  private def snapshotDeleteReq(persistenceId: String, sequenceNr: Long): WriteRequest = {
    new WriteRequest().withDeleteRequest(new DeleteRequest().withKey {
      val item: Item = new JHMap
      item.put(Key, S(messagePartitionKey(persistenceId)))
      item.put(SequenceNr, N(sequenceNr))
      item
    })
  }

  def save(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Future[Unit] = {
    dynamo.putItem(putItem(toSnapshotItem(persistenceId, sequenceNr, timestamp, snapshot)))
      .map { _ =>
        ()
      }
  }

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {

    loadQueryResult(persistenceId, criteria)
      .map { result =>
        val results = result.getItems.asScala.map(item => (item.get(Key).getS, item.get(SequenceNr).getN.toLong))
        result.getItems.asScala.headOption
          .map(youngest =>
            fromSnapshotItem(persistenceId, youngest))
      }
  }

  private def loadQueryResult(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[QueryResult] = {
    criteria match {
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) if minSequenceNr == 0 && maxSequenceNr == Long.MaxValue =>
        loadByTimestamp(persistenceId, minTimestamp = minTimestamp, maxTimestamp = maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) if minTimestamp == 0 && maxTimestamp == Long.MaxValue =>
        loadBySeqNr(persistenceId, minSequenceNr = minSequenceNr, maxSequenceNr = maxSequenceNr)
      case _ =>
        loadByBoth(persistenceId, criteria)

    }
  }

  private def loadByTimestamp(persistenceId: String, minTimestamp: Long, maxTimestamp: Long): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withIndexName(TimestampIndex)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $Timestamp BETWEEN :tsMinVal AND :tsMaxVal ")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":tsMinVal", N(minTimestamp))
      .addExpressionAttributeValuesEntry(":tsMaxVal", N(maxTimestamp))
      .withScanIndexForward(false)

    dynamo.query(request)
  }

  private def loadBySeqNr(persistenceId: String, minSequenceNr: Long, maxSequenceNr: Long): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $SequenceNr BETWEEN :seqMinVal AND :seqMaxVal")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":seqMinVal", N(minSequenceNr))
      .addExpressionAttributeValuesEntry(":seqMaxVal", N(maxSequenceNr))
      .withScanIndexForward(false)

    dynamo.query(request)
  }

  private def loadByBoth(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[QueryResult] = {
    val request = new QueryRequest()
      .withTableName(Table)
      .withKeyConditionExpression(s" $Key = :partitionKeyVal AND $SequenceNr BETWEEN :seqMinVal AND :seqMaxVal")
      .addExpressionAttributeValuesEntry(":partitionKeyVal", S(messagePartitionKey(persistenceId)))
      .addExpressionAttributeValuesEntry(":seqMinVal", N(criteria.minSequenceNr))
      .addExpressionAttributeValuesEntry(":seqMaxVal", N(criteria.maxSequenceNr))
      .withScanIndexForward(false)
      .withFilterExpression(s"$Timestamp BETWEEN :tsMinVal AND :tsMaxVal ")
      .addExpressionAttributeValuesEntry(":tsMinVal", N(criteria.minTimestamp))
      .addExpressionAttributeValuesEntry(":tsMaxVal", N(criteria.maxTimestamp))

    dynamo.query(request)
  }

  private def toSnapshotItem(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Item = {
    val item: Item = new JHMap

    item.put(Key, S(messagePartitionKey(persistenceId)))
    item.put(SequenceNr, N(sequenceNr))
    item.put(Timestamp, N(timestamp))
    val snp = B(serialization.serialize(Snapshot(snapshot)).get)
    item.put(Payload, snp)
    item
  }

  private def fromSnapshotItem(persistenceId: String, item: Item): SelectedSnapshot = {
    val seqNr = item.get(SequenceNr).getN.toLong
    val timestamp = item.get(Timestamp).getN.toLong
    val payloadValue = item.get(Payload).getB
    serialization.deserialize(payloadValue.array(), classOf[Snapshot])
      .map(snapshot =>
        SelectedSnapshot(metadata = SnapshotMetadata(persistenceId, sequenceNr = seqNr, timestamp = timestamp), snapshot = snapshot.data)).get
  }

  private def messagePartitionKey(persistenceId: String): String =
    s"$JournalName-P-$persistenceId"

}
