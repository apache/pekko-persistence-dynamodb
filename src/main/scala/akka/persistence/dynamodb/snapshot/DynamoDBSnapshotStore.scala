package akka.persistence.dynamodb.snapshot

import akka.actor.ActorLogging
import akka.persistence.dynamodb.journal.{ DynamoDBRequests, dynamoClient }
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.SerializationExtension
import com.typesafe.config.Config

import scala.concurrent.Future

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore /* with DynamoDBRequests */ with ActorLogging {
  val settings = new DynamoDBSnapshotConfig(config)
  val dynamo = dynamoClient(context.system, settings)
  val serialization = SerializationExtension(context.system)

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! DynamoDBSnapshotStore.Init
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future.successful(None)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = Future.successful(())

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful(())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful(())

}

object DynamoDBSnapshotStore {
  private case object Init
}