package akka.persistence.journal.dynamodb

import DynamoDBJournal._
import akka.actor.ActorSystem
import akka.persistence.journal.JournalSpec
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.{Suite, BeforeAndAfterEach}

trait DynamoDBSpec extends BeforeAndAfterEach {
  this:TestKit with Suite =>
  override def beforeEach(): Unit = {
    val config = system.settings.config.getConfig(Conf)
    val table = config.getString(JournalName)
    val client = dynamoClient(system, system, config)
    val create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(DynamoDBJournal.schema)
      .withAttributeDefinitions(DynamoDBJournal.schemaAttributes)
      .withProvisionedThroughput(new ProvisionedThroughput(10, 10))
    import system.dispatcher

    val setup = client.sendListTables(new ListTablesRequest()).flatMap {
      list =>
        if (list.getTableNames.size() > 0) {
          client.sendDeleteTable(new DeleteTableRequest(table)).flatMap {
            res =>
              client.sendCreateTable(create).map(_ => ())
          }
        } else {
          client.sendCreateTable(create).map(_ => ())
        }
    }
    Await.result(setup, 5 seconds)
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }
}

class DynamoDBJournalSpec extends TestKit(ActorSystem("test")) with JournalSpec with DynamoDBSpec
