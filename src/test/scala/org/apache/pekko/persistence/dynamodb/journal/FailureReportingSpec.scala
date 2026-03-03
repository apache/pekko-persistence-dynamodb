/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb.journal

import org.apache.pekko
import pekko.actor._
import pekko.event.Logging
import pekko.persistence.JournalProtocol._
import pekko.persistence._
import pekko.persistence.dynamodb._
import pekko.testkit._
import software.amazon.awssdk.services.dynamodb.model._
import com.typesafe.config.ConfigFactory
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class FailureReportingSpec
    extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec {

  implicit val patience: PatienceConfig = PatienceConfig(5.seconds)

  override val persistenceId = "FailureReportingSpec"

  val bigMsg = Array.tabulate[Byte](400000)(_.toByte)

  def expectRejection(msg: String, repr: PersistentRepr) = {
    val rej = expectMsgType[WriteMessageRejected]
    rej.message should ===(repr)
    rej.cause shouldBe a[DynamoDBJournalRejection]
    (rej.cause.getMessage should include).regex(msg)
  }

  def expectFailure[T: Manifest](msg: String, repr: PersistentRepr) = {
    val rej = expectMsgType[WriteMessageFailure]
    rej.message should ===(repr)
    rej.cause shouldBe a[T]
    (rej.cause.getMessage should include).regex(msg)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    dynamo.shutdown()
    system.terminate().futureValue
    super.afterAll()
  }

  "DynamoDB Journal Failure Reporting" must {

    "notify user about absent journal table" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal.journal-table=ThisTableDoesNotExist")
        .withFallback(ConfigFactory.load())
      implicit val system: ActorSystem = ActorSystem("FailureReportingSpec-test1", config)
      try EventFilter[ResourceNotFoundException](pattern = ".*ThisTableDoesNotExist.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
      finally system.terminate()
    }

    "keep running even when journal table is absent" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal.journal-table=ThisTableDoesNotExist")
        .withFallback(ConfigFactory.load())
      implicit val system = ActorSystem("FailureReportingSpec-test2", config)
      try {
        val journal =
          EventFilter[ResourceNotFoundException](pattern = ".*ThisTableDoesNotExist.*", occurrences = 1).intercept {
            EventFilter.error(pattern = ".*requests will fail.*ThisTableDoesNotExist.*", occurrences = 1).intercept {
              Persistence(system).journalFor("")
            }
          }
        EventFilter[ResourceNotFoundException](pattern = ".*BatchGetItemRequest.*", occurrences = 1).intercept {
          EventFilter[DynamoDBJournalFailure](
            pattern = s".*failed.*read-highest-sequence-number.*$persistenceId.*",
            occurrences = 1).intercept {
            journal ! ReplayMessages(0, Long.MaxValue, 0, persistenceId, testActor)
            expectMsgType[ReplayMessagesFailure]
          }
        }
      } finally system.terminate()
    }

    "notify user about used config" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal{log-config=on}")
        .withFallback(ConfigFactory.load())
      implicit val system = ActorSystem("FailureReportingSpec-test3", config)
      try EventFilter.info(pattern = ".*DynamoDBJournalConfig.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
      finally system.terminate()
    }

    "not notify user about config errors when starting the default journal" in {
      val config = ConfigFactory.parseString("""
dynamodb-journal {
  endpoint = "http://localhost:8888"
  aws-access-key-id = "AWS_ACCESS_KEY_ID"
  aws-secret-access-key = "AWS_SECRET_ACCESS_KEY"
}
pekko.persistence.journal.plugin = "dynamodb-journal"
pekko.persistence.snapshot-store.plugin = "no-snapshot-store"
pekko.loggers = ["org.apache.pekko.testkit.TestEventListener"]
""")
      implicit val system = ActorSystem("FailureReportingSpec-test4", config)
      try {
        val probe = TestProbe()
        system.eventStream.subscribe(probe.ref, classOf[Logging.LogEvent])
        EventFilter[ResourceNotFoundException](pattern = ".*pekko-persistence.*", occurrences = 1).intercept {
          Persistence(system).journalFor("")
        }
        probe.expectMsgType[Logging.Error].message.toString should include("DescribeTableRequest(pekko-persistence)")
        probe.expectMsgType[Logging.Error].message.toString should include("until the table 'pekko-persistence'")
        probe.expectNoMessage(0.seconds)
      } finally system.terminate()
    }

    "properly fail whole writes" in {
      val config = ConfigFactory
        .parseString("my-dynamodb-journal.journal-table=ThisTableDoesNotExist")
        .withFallback(ConfigFactory.load())
      implicit val system = ActorSystem("FailureReportingSpec-test5", config)
      try {
        val journal =
          EventFilter[ResourceNotFoundException](pattern = ".*ThisTableDoesNotExist.*", occurrences = 1).intercept {
            EventFilter.error(pattern = ".*requests will fail.*ThisTableDoesNotExist.*", occurrences = 1).intercept {
              Persistence(system).journalFor("")
            }
          }

        val msgs = (1 to 3).map(i => persistentRepr(f"w-$i"))

        EventFilter[ResourceNotFoundException](pattern = ".*ThisTableDoesNotExist.*", occurrences = 1).intercept {
          journal ! WriteMessages(AtomicWrite(msgs(0)) :: Nil, testActor, 42)
          expectMsgType[WriteMessagesFailed].cause shouldBe a[DynamoDBJournalFailure]
          expectFailure[DynamoDBJournalFailure]("ThisTableDoesNotExist", msgs(0))
        }

        EventFilter[ResourceNotFoundException](pattern = ".*ThisTableDoesNotExist.*", occurrences = 1).intercept {
          journal ! WriteMessages(AtomicWrite(msgs(1)) :: AtomicWrite(msgs(2)) :: Nil, testActor, 42)
          expectMsgType[WriteMessagesFailed].cause shouldBe a[DynamoDBJournalFailure]
          expectFailure[DynamoDBJournalFailure]("ThisTableDoesNotExist", msgs(1))
          expectFailure[DynamoDBJournalFailure]("ThisTableDoesNotExist", msgs(2))
        }
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

      EventFilter[DynamoDBJournalRejection](occurrences = 2).intercept {
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

    "have sensible error messages" when {
      val keyItem = Map(Key -> S("TheKey"), Sort -> N("42")).asJava
      val key2Item = Map(Key -> S("The2Key"), Sort -> N("43")).asJava

      "reporting table problems" in {
        val aws = DescribeTableRequest.builder().tableName("TheTable").build()
        dynamo.describeTable(aws).failed.futureValue.getMessage should (include("DescribeTable") or include("TheTable"))
      }

      "reporting putItem problems" in {
        val aws = PutItemRequest.builder().tableName("TheTable").item(keyItem).build()
        dynamo.putItem(aws).failed.futureValue.getMessage should (include("PutItem") or include("TheTable"))
      }

      "reporting deleteItem problems" in {
        val aws = DeleteItemRequest.builder().tableName("TheTable").key(keyItem).build()
        dynamo.deleteItem(aws).failed.futureValue.getMessage should (include("DeleteItem") or include("TheTable"))
      }

      "reporting query problems" in {
        val aws = QueryRequest.builder().tableName("TheTable")
          .keyConditionExpression(":kkey = par")
          .expressionAttributeValues(Map(":kkey" -> S("TheKey")).asJava).build()
        dynamo.query(aws).failed.futureValue.getMessage should (include("Query") or include("TheTable"))
      }

      "reporting batch write problems" in {
        val write = WriteRequest.builder().putRequest(PutRequest.builder().item(keyItem).build()).build()
        val remove = WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(key2Item).build()).build()
        val aws = BatchWriteItemRequest.builder()
          .requestItems(Map("TheTable" -> Seq(write, remove).asJava).asJava).build()
        dynamo.batchWriteItem(aws).failed.futureValue.getMessage should (include("BatchWriteItem") or include("TheTable"))
      }

      "reporting batch read problems" in {
        val ka = KeysAndAttributes.builder().keys(keyItem, key2Item).build()
        val aws = BatchGetItemRequest.builder().requestItems(Map("TheTable" -> ka).asJava).build()
        dynamo.batchGetItem(aws).failed.futureValue.getMessage should (include("BatchGetItem") or include("TheTable"))
      }
    }

  }

}
