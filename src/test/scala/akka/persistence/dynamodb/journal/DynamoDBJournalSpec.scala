/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.persistence.journal.JournalSpec
import com.amazonaws.services.dynamodbv2.model.{ CreateTableRequest, DeleteTableRequest, ListTablesRequest, ProvisionedThroughput }
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.persistence.CapabilityFlag
import scala.concurrent.Future
import akka.pattern.extended.ask
import akka.actor.ActorRef

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) with DynamoDBUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
  }

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    Await.result(journal ? (Purge(pid, _)), 5.seconds)
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.off()
}
