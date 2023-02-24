/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package org.apache.pekko.persistence.dynamodb.journal

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.extended.ask
import org.apache.pekko.persistence.CapabilityFlag
import org.apache.pekko.persistence.dynamodb.IntegSpec
import org.apache.pekko.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) with DynamoDBUtils with IntegSpec {

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
