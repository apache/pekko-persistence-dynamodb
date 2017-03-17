package akka.persistence.dynamodb.snapshot
import java.util.{ Collections, HashMap => JHMap, List => JList, Map => JMap }

import akka.persistence.dynamodb.journal.{ N, Sort, _ }
import akka.persistence.serialization.Snapshot

import scala.concurrent.Future

trait DynamoDBSnapshotRequests extends DynamoDBRequests {
  this: DynamoDBSnapshotStore =>

  import settings._
  import context.dispatcher

  def save(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Future[Unit] = {
    dynamo.putItem(putItem(toSnapshotItem(persistenceId, sequenceNr, timestamp, snapshot)))
      .map { _ =>
        ()
      }
  }

  private def toSnapshotItem(persistenceId: String, sequenceNr: Long, timestamp: Long, snapshot: Any): Item = {
    val item: Item = new JHMap

    item.put(Key, S(messagePartitionKey(persistenceId, sequenceNr)))
    item.put(Sort, N(sequenceNr))
    item.put(SequenceNr, N(sequenceNr))
    item.put(Timestamp, N(timestamp))
    val snp = B(serialization.serialize(Snapshot(snapshot)).get)
    item.put(Payload, snp)
    item
  }
}
