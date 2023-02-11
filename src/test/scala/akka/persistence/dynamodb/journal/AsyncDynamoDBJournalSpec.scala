/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor.ActorRef
import akka.pattern.extended.ask
import akka.persistence.CapabilityFlag
import akka.persistence.dynamodb.IntegSpec
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

object AsyncDynamoDBJournalSpec {

  val config = ConfigFactory
    .parseString("""
      |akka.actor {
      |  allow-java-serialization = on
      |  serializers {
      |    test = "akka.persistence.dynamodb.journal.TestSerializer"
      |  }
      |  serialization-bindings {
      |    "java.io.Serializable" = test
      |  }
      |}
    """.stripMargin)
    .withFallback(ConfigFactory.load())

}

class AsyncDynamoDBJournalSpec extends JournalSpec(AsyncDynamoDBJournalSpec.config) with DynamoDBUtils with IntegSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    dynamo.shutdown()
    super.afterAll()
  }

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    Await.result(journal ? (Purge(pid, _)), 5.seconds)
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.on()
}
