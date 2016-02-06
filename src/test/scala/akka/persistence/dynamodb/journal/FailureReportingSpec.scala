/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalactic.ConversionCheckedTripleEquals
import akka.actor._
import akka.testkit._
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.persistence._
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import akka.event.Logging
import akka.persistence.JournalProtocol._
import java.util.UUID

object FailureReportingSpec {
  class GuineaPig(report: ActorRef) extends PersistentActor {
    def persistenceId = context.self.path.name
    def receiveRecover = {
      case x => report ! x
    }
    def receiveCommand = {
      case x => persist(x)(_ => report ! x)
    }
  }
}

class FailureReportingSpec extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with ConversionCheckedTripleEquals
    with DynamoDBUtils {

  implicit val patience = PatienceConfig(5.seconds)

  override val persistenceId = "FailureReportingSpec"

  val bigMsg = Array.tabulate[Byte](400000)(_.toByte)

  def expectRejection(msg: String, repr: PersistentRepr) = {
    val rej = expectMsgType[WriteMessageRejected]
    rej.message should ===(repr)
    rej.cause shouldBe a[DynamoDBJournalRejection]
    rej.cause.getMessage should include regex msg
  }

  override def beforeAll(): Unit = ensureJournalTableExists()
  override def afterAll(): Unit = system.terminate().futureValue

  "DynamoDB Journal Failure Reporting" must {

    "notify user about absent journal table" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal.journal-table=nonexistent")
        .withFallback(ConfigFactory.load())
      implicit val system = ActorSystem("FailureReportingSpec-test1", config)
      try
        EventFilter[ResourceNotFoundException](pattern = ".*nonexistent.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
      finally system.terminate()
    }

    "notify user about used config" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal{log-config=on\naws-client-config.protocol=HTTPS}")
        .withFallback(ConfigFactory.load())
      implicit val system = ActorSystem("FailureReportingSpec-test1", config)
      try
        EventFilter.info(pattern = ".*protocol:https.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
      finally system.terminate()
    }

    "not notify user about config errors when starting the default journal" in {
      val config = ConfigFactory.parseString("""
dynamodb-journal.endpoint = "http://localhost:8000"
akka.persistence.journal.plugin = "dynamodb-journal"
akka.persistence.snapshot-store.plugin = "no-snapshot-store"
akka.loggers = ["akka.testkit.TestEventListener"]
""")
      implicit val system = ActorSystem("FailureReportingSpec-test2", config)
      try {
        val probe = TestProbe()
        system.eventStream.subscribe(probe.ref, classOf[Logging.LogEvent])
        EventFilter[ResourceNotFoundException](pattern = ".*akka-persistence.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
        probe.expectMsgType[Logging.Error]
        probe.expectNoMsg(0.seconds)
      } finally system.terminate()
    }

    "properly reject too large payloads" in {
      val journal = Persistence(system).journalFor("")
      val msgs = Vector("t-1", bigMsg, "t-3", "t-4", bigMsg, "t-6").map(persistentRepr)
      val write =
        AtomicWrite(msgs(0)) ::
          AtomicWrite(msgs(1)) ::
          AtomicWrite(msgs(2)) ::
          AtomicWrite(msgs(3) :: msgs(4) :: Nil) ::
          AtomicWrite(msgs(5)) ::
          Nil

      EventFilter[DynamoDBJournalRejection](occurrences = 2) intercept {
        journal ! WriteMessages(write, testActor, 42)
        expectMsg(WriteMessagesSuccessful)
        expectMsg(WriteMessageSuccess(msgs(0), 42))
        expectRejection("MaxItemSize exceeded", msgs(1))
        expectMsg(WriteMessageSuccess(msgs(2), 42))
        expectRejection("AtomicWrite rejected as a whole.*MaxItemSize exceeded", msgs(3))
        expectRejection("AtomicWrite rejected as a whole.*MaxItemSize exceeded", msgs(4))
        expectMsg(WriteMessageSuccess(msgs(5), 42))
      }

      journal ! ReplayMessages(msgs.head.sequenceNr, msgs.last.sequenceNr, Long.MaxValue, persistenceId, testActor)
      expectMsg(ReplayedMessage(msgs(0)))
      expectMsg(ReplayedMessage(msgs(2)))
      expectMsg(ReplayedMessage(msgs(5)))
      expectMsgType[RecoverySuccess]
    }

  }

}
