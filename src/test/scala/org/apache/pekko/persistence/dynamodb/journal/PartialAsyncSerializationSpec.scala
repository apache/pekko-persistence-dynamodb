/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.dynamodb.journal

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.JournalProtocol._
import org.apache.pekko.persistence._
import org.apache.pekko.persistence.dynamodb.IntegSpec
import org.apache.pekko.testkit._
import com.typesafe.config.ConfigFactory
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

trait SerializeAsync

case class TestAsyncMessage(sequence: Int) extends SerializeAsync

object PartialAsyncSerializationSpec {
  val config = ConfigFactory
    .parseString("""
      |pekko.actor {
      |  allow-java-serialization = on
      |  serializers {
      |    test = "org.apache.pekko.persistence.dynamodb.journal.TestSerializer"
      |  }
      |  serialization-bindings {
      |    "org.apache.pekko.persistence.dynamodb.journal.SerializeAsync" = test
      |  }
      |}
    """.stripMargin)
    .withFallback(ConfigFactory.load())
}

class PartialAsyncSerializationSpec
    extends TestKit(ActorSystem("PartialAsyncSerializationSpec", PartialAsyncSerializationSpec.config))
    with ImplicitSender
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }
  override def afterAll(): Unit = {
    dynamo.shutdown()
    system.terminate().futureValue
    super.afterAll()
  }

  override val persistenceId = "PartialAsyncSerializationSpec"
  lazy val journal = Persistence(system).journalFor("")

  "DynamoDB Journal (Serialization Test)" must {

    val messages = 20
    val writes = (1 to messages).map(i =>
      if (i % 2 == 0) {
        AtomicWrite(persistentRepr(TestAsyncMessage(i)))
      } else {
        AtomicWrite(persistentRepr(f"a-$i%04d"))
      })
    val probe = TestProbe()

    s"should successfully serialize using async and not async serializers" in {

      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! WriteMessages(writes, testActor, 1)
      journal ! ReplayMessages(1, 0, Long.MaxValue, persistenceId, probe.ref)
      expectMsg(WriteMessagesSuccessful)
      (1 to messages).foreach(i => {
        val msg = expectMsgType[WriteMessageSuccess]
        msg.persistent.sequenceNr.toInt should ===(i)
        if (i % 2 == 0) {
          msg.persistent.payload should ===(TestAsyncMessage(i))
        }
      })
      probe.expectMsg(RecoverySuccess(messages))
    }

  }

}
