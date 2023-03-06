/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
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

object AsyncDynamoDBJournalSpec {

  val config = ConfigFactory
    .parseString("""
      |pekko.actor {
      |  allow-java-serialization = on
      |  serializers {
      |    test = "org.apache.pekko.persistence.dynamodb.journal.TestSerializer"
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
