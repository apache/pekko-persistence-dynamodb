package akka.persistence.dynamodb.query.scaladsl.internal

import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb._
import akka.persistence.dynamodb.journal._
import akka.persistence.dynamodb.query.scaladsl.internal.{
  DynamodbCurrentEventsByPersistenceIdQuery => InternalDynamodbCurrentEventsByPersistenceIdQuery,
  DynamodbCurrentPersistenceIdsQuery => InternalDynamodbCurrentPersistenceIdsQuery
}
import akka.persistence.dynamodb.query.scaladsl.{ DynamodbReadJournal => PublicDynamodbReadJournal }
import akka.persistence.dynamodb.query.{ DynamoDBReadJournalConfig, ReadJournalSettingsProvider }
import akka.persistence.query.scaladsl.ReadJournal
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.{ Materializer, SystemMaterializer }
import com.typesafe.config.Config

class DynamodbReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with PublicDynamodbReadJournal
    with InternalDynamodbCurrentEventsByPersistenceIdQuery
    with InternalDynamodbCurrentPersistenceIdsQuery
    with ReadJournalSettingsProvider
    with JournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider
    with ActorSystemLoggingProvider {

  protected val readJournalSettings       = new DynamoDBReadJournalConfig(config)
  protected val dynamo: DynamoDBHelper    = dynamoClient(system, readJournalSettings)
  val serialization: Serialization        = SerializationExtension(system)
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  val journalSettings                     = new DynamoDBJournalConfig(config)

  def close(): Unit = dynamo.shutdown()
}
