/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.persistence.journal.JournalSpec
import akka.persistence.dynamodb.journal.DynamoDBJournal._
import com.amazonaws.services.dynamodbv2.model.{ CreateTableRequest, DeleteTableRequest, ListTablesRequest, ProvisionedThroughput }
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.persistence.CapabilityFlag
import scala.concurrent.Future

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.journal.plugin"))
    val settings = new DynamoDBJournalConfig(config)
    val table = settings.JournalTable
    val client = dynamoClient(system, config)
    val create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(DynamoDBJournal.schema)
      .withAttributeDefinitions(DynamoDBJournal.schemaAttributes)
      .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
    import system.dispatcher

    val setup = for {
      list <- client.sendListTables(new ListTablesRequest())
      _ <- {
        if (list.getTableNames.contains(table))
          client.sendDeleteTable(new DeleteTableRequest(table))
        else Future.successful(())
      }
      c <- client.sendCreateTable(create)
    } yield c
    Await.result(setup, 5 seconds)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.on()
}
