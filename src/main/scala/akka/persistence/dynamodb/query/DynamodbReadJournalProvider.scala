package akka.persistence.dynamodb.query

import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb.query.javadsl.{ DynamodbReadJournal => JavaDynamodbReadJournal }
import akka.persistence.dynamodb.query.scaladsl.internal.{ DynamodbReadJournal => ScalaDynamodbReadJournal }
import akka.persistence.query.ReadJournalProvider
import akka.persistence.query.javadsl.{ ReadJournal => JavaReadJournal }
import akka.persistence.query.scaladsl.{ ReadJournal => ScalaReadJournal }
import com.typesafe.config.Config

class DynamodbReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  private lazy val _scalaReadJournal                   = new ScalaDynamodbReadJournal(config, configPath)(system)
  override def scaladslReadJournal(): ScalaReadJournal = _scalaReadJournal

  private lazy val _javadslReadJournal               = new JavaDynamodbReadJournal(_scalaReadJournal)
  override def javadslReadJournal(): JavaReadJournal = _javadslReadJournal
}

trait ReadJournalSettingsProvider {
  protected def readJournalSettings: DynamoDBReadJournalConfig
}
