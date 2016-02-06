/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.actor.ActorSystem
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.testkit._
import akka.persistence.journal.AsyncWriteTarget.ReplaySuccess

class RecoveryConsistencySpec extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ConversionCheckedTripleEquals
    with DynamoDBUtils {

  override def beforeAll(): Unit = ensureJournalTableExists()
  override def afterAll(): Unit = system.terminate().futureValue

  override val persistenceId = "RecoveryConsistencySpec"
  val journal = Persistence(system).journalFor("")

  "DynamoDB Journal (Recovery)" must {

    val N = 50
    val M = 20
    val writes = (1 to M).map(i => AtomicWrite(persistentRepr(f"a-$i%04d")))
    val probe = TestProbe()

    for (i <- 1 to N) s"not return intermediate values for the highest sequence number ($i of $N)" in {
      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! WriteMessages(writes, testActor, 1)
      journal ! ReplayMessages(1, 0, Long.MaxValue, persistenceId, probe.ref)
      expectMsg(WriteMessagesSuccessful)
      (1 to M) foreach (_ => expectMsgType[WriteMessageSuccess])
      probe.expectMsg(RecoverySuccess(M))
    }

  }
}
