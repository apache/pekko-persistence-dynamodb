/**
 * Copyright (C) 2021 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor.ActorSystem
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.persistence.dynamodb._
import akka.testkit._
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class PersistAllConsistencySpec
    extends TestKit(ActorSystem("PersistAllConsistencySpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ConversionCheckedTripleEquals
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
      println(s"start: ${start}; end: ${end}")
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
        println(s"start: ${start1}; end: ${end1}")
        val padding = AtomicWrite((start1 to end1).map(i => persistentRepr(f"h-$i"))) :: Nil

        journal ! WriteMessages(padding, testActor, 1)
        expectMsg(WriteMessagesSuccessful)
        (start1 to end1).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

        val start2 = nextSeqNr
        val end2 = start2 + 2
        println(s"start: ${start2}; end: ${end2}")
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
      println(s"start: ${start}; end: ${end}")
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
