/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalactic.ConversionCheckedTripleEquals
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.testkit.EventFilter
import akka.persistence.Persistence
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException

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
        EventFilter[ResourceNotFoundException](pattern = ".*TableName: nonexistent.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
      finally system.terminate()
    }

  }

}
