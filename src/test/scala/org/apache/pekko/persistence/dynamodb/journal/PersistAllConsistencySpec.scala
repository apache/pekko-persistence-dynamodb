/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/**
 * Copyright (C) 2021 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb.journal

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.JournalProtocol._
import org.apache.pekko.persistence._
import org.apache.pekko.persistence.dynamodb.IntegSpec
import org.apache.pekko.testkit._
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class PersistAllConsistencySpec
    extends TestKit(ActorSystem("PersistAllConsistencySpec"))
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

  override val persistenceId = "PersistAllConsistencySpec"
  lazy val journal = Persistence(system).journalFor("")

  "DynamoDB Journal (persistAll)" must {

    "recover correctly if the first write is a batch" in {
      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))

      val start = nextSeqNr
      val end = 10
      println(s"start: $start; end: $end")
      val padding = AtomicWrite((start to end).map(i => persistentRepr(f"h-$i"))) :: Nil

      journal ! WriteMessages(padding, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (start to end).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      journal ! ReplayMessages(start, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      (start to end).foreach(i => expectMsg(ReplayedMessage(generatedMessages(i))))
      expectMsg(RecoverySuccess(end))
    }

    for (t <- Seq(("last", 3), ("middle", 2), ("first", 1)))
      s"correctly cross page boundaries with AtomicWrite position ${t._1}" in {
        val start1 = nextSeqNr
        val end1 = ((start1 / PartitionSize) + 1) * PartitionSize - t._2
        println(s"start: $start1; end: $end1")
        val padding = AtomicWrite((start1 to end1).map(i => persistentRepr(f"h-$i"))) :: Nil

        journal ! WriteMessages(padding, testActor, 1)
        expectMsg(WriteMessagesSuccessful)
        (start1 to end1).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

        val start2 = nextSeqNr
        val end2 = start2 + 2
        println(s"start: $start2; end: $end2")
        val subject = AtomicWrite((start2 to end2).map(i => persistentRepr(f"h-$i"))) :: Nil

        journal ! WriteMessages(subject, testActor, 1)
        expectMsg(WriteMessagesSuccessful)
        (start2 to end2).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

        journal ! ReplayMessages(start1, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
        (start1 to end2).foreach(i => expectMsg(ReplayedMessage(generatedMessages(i))))
        expectMsg(RecoverySuccess(end2))
      }

    s"recover correctly when the last partition event ends on ${PartitionSize - 1}" in {
      val start = nextSeqNr
      val end = ((start / PartitionSize) + 1) * PartitionSize - 1
      println(s"start: $start; end: $end")
      val padding = AtomicWrite((start to end).map(i => persistentRepr(f"h-$i"))) :: Nil

      journal ! WriteMessages(padding, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (start to end).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      journal ! ReplayMessages(start, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      (start to end).foreach(i => expectMsg(ReplayedMessage(generatedMessages(i))))
      expectMsg(RecoverySuccess(end))
    }

  }

}
