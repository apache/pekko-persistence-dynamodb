/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.testkit._
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.actor.ActorSystem
import akka.persistence._
import akka.persistence.JournalProtocol._
import java.util.UUID

class DeletionSpec extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ConversionCheckedTripleEquals
    with DynamoDBUtils {

  override def beforeAll(): Unit = ensureJournalTableExists()
  override def afterAll(): Unit = {
    system.terminate().futureValue
    client.shutdown()
  }

  override val persistenceId = "DeletionSpec"
  val journal = Persistence(system).journalFor("")

  "DynamoDB Journal (Deletion)" must {

    "1 purge events" in {
      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set.empty, Set.empty, Nil))
    }

    "2 store events" in {
      val msgs = (0 to 9).map(i => AtomicWrite(persistentRepr(s"a-$i")))
      journal ! WriteMessages(msgs, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (0 to 9) foreach (_ => expectMsgType[WriteMessageSuccess])
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set.empty, (1L to 10).toSet, (1L to 10)))
    }

    "3 delete some events" in {
      journal ! DeleteMessagesTo(persistenceId, 5L, testActor)
      expectMsg(DeleteMessagesSuccess(5L))
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set(6L), (1L to 10).toSet, (6L to 10)))
    }

    "4 delete no events" in {
      journal ! DeleteMessagesTo(persistenceId, 3L, testActor)
      expectMsg(DeleteMessagesSuccess(3L))
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set(6L), (1L to 10).toSet, (6L to 10)))
    }

    "5 delete all events" in {
      journal ! DeleteMessagesTo(persistenceId, 31L, testActor)
      expectMsg(DeleteMessagesSuccess(31L))
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set(6L, 11L), (1L to 10).toSet, Nil))
    }

    "6 purge events" in {
      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! ListAll(persistenceId, testActor)
      expectMsg(ListAllResult(persistenceId, Set.empty, Set.empty, Nil))
    }

  }

}
