package akka.persistence.dynamodb.snapshot

import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.snapshot.SnapshotStore
import com.typesafe.config.Config

import scala.concurrent.Future

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore {
  val settings = new DynamoDBSnapshotConfig(config)

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future.successful(None)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = Future.successful(())

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful(())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful(())
}
