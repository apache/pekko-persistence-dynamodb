package akka.persistence.dynamodb.snapshot

import akka.actor.ActorLogging
import akka.persistence.dynamodb.journal._
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.SerializationExtension
import com.typesafe.config.Config
import java.util.{ HashMap => JHMap, Map => JMap }
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

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future.successful(None)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    save(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, snapshot)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful(())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful(())

  def keyLength(persistenceId: String, sequenceNr: Long): Int =
    persistenceId.length + JournalName.length + KeyPayloadOverhead

  def messageKey(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap
    item.put(Key, S(messagePartitionKey(persistenceId, sequenceNr)))
    item.put(Sort, N(sequenceNr % 100))
    item
  }

  def messagePartitionKey(persistenceId: String, sequenceNr: Long): String =
    s"$JournalName-P-$persistenceId-${sequenceNr / 100}"

}

object DynamoDBSnapshotStore {
  private case object Init
}