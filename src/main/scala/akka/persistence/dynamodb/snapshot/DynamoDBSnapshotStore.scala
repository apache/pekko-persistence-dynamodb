package akka.persistence.dynamodb.snapshot

import akka.actor.ActorLogging
import akka.persistence.dynamodb.journal._
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.SerializationExtension
import com.typesafe.config.Config
import java.util.{ HashMap => JHMap, Map => JMap }

import akka.persistence.dynamodb.journal.DynamoDBProvider
import akka.persistence.dynamodb._
import scala.concurrent.Future

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore with DynamoDBSnapshotRequests with ActorLogging with DynamoDBProvider {
  override val settings = new DynamoDBSnapshotConfig(config)
  override val dynamo = dynamoClient(context.system, settings)
  override val serialization = SerializationExtension(context.system)
  import settings._

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! DynamoDBSnapshotStore.Init
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    load(persistenceId, criteria)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    save(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, snapshot)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    delete(metadata)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    delete(persistenceId, criteria)
  }

  def keyLength(persistenceId: String, sequenceNr: Long): Int =
    persistenceId.length + JournalName.length + KeyPayloadOverhead

  //TODO remove this. unused
  def messageKey(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap
    item.put(Key, S(messagePartitionKey(persistenceId, sequenceNr)))
    item.put(Sort, N(sequenceNr % 100))
    item
  }

  //TODO remove this. unused
  def messagePartitionKey(persistenceId: String, sequenceNr: Long): String =
    s"$JournalName-P-$persistenceId-"

}

object DynamoDBSnapshotStore {
  private case object Init
}