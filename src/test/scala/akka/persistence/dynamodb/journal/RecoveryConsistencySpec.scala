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
import com.amazonaws.services.dynamodbv2.model._
import java.util.{ HashMap => JHMap }

class RecoveryConsistencySpec extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ConversionCheckedTripleEquals
    with DynamoDBUtils {

  override def beforeAll(): Unit = ensureJournalTableExists()
  override def afterAll(): Unit = {
    client.shutdown()
    system.terminate().futureValue
  }

  override val persistenceId = "RecoveryConsistencySpec"
  lazy val journal = Persistence(system).journalFor("")

  import settings._

  "DynamoDB Journal (Recovery)" must {

    val repetitions = 50
    val messages = 20
    val writes = (1 to messages).map(i => AtomicWrite(persistentRepr(f"a-$i%04d")))
    val probe = TestProbe()

    for (i <- 1 to repetitions) s"not return intermediate values for the highest sequence number ($i of $repetitions)" in {
      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! WriteMessages(writes, testActor, 1)
      journal ! ReplayMessages(1, 0, Long.MaxValue, persistenceId, probe.ref)
      expectMsg(WriteMessagesSuccessful)
      (1 to messages) foreach (i => expectMsgType[WriteMessageSuccess].persistent.sequenceNr should ===(i))
      probe.expectMsg(RecoverySuccess(messages))
    }

    "only replay completely persisted AtomicWrites" in {
      val more =
        AtomicWrite((1 to 3).map(i => persistentRepr(f"b-$i"))) :: // hole in the middle
          AtomicWrite((4 to 6).map(i => persistentRepr(f"b-$i"))) :: // hole in the beginning
          AtomicWrite((7 to 9).map(i => persistentRepr(f"b-$i"))) :: // no hole
          AtomicWrite((10 to 12).map(i => persistentRepr(f"b-$i"))) :: // hole in the end
          AtomicWrite((13 to 15).map(i => persistentRepr(f"b-$i"))) :: // hole in the end
          AtomicWrite(persistentRepr("c")) ::
          Nil
      journal ! WriteMessages(more, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (messages + 1 to messages + 16) foreach (i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      Seq(2, 4, 12, 15) foreach (i => delete(messages + i))

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      for {
        i <- 1 to (messages + 16)
        if (i <= messages || (i >= (messages + 7) && i <= (messages + 9)) || i == (messages + 16))
      } expectMsg(ReplayedMessage(generatedMessages(i)))
      expectMsg(RecoverySuccess(messages + 16))
    }

  }

  private def delete(num: Long) = {
    val key: Item = new JHMap
    key.put(Key, S(s"$JournalName-P-$persistenceId-${num / 100}"))
    key.put(Sort, N(num % 100))
    client.deleteItem(new DeleteItemRequest().withTableName(JournalTable).withKey(key)).futureValue
  }
}
