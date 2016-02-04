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
  with ConversionCheckedTripleEquals {

  implicit val patience = PatienceConfig(5.seconds)

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

  }

}
