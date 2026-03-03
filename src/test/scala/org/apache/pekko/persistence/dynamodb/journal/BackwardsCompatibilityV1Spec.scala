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
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, PutItemRequest }
import com.typesafe.config.ConfigFactory
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.net.URI
import java.util.Base64
import scala.jdk.CollectionConverters._

class BackwardsCompatibilityV1Spec
    extends TestKit(ActorSystem("BackwardsCompatibilityV1Spec"))
    with ImplicitSender
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec {

  def loadV1VersionData(): Unit = {
    val config = ConfigFactory.load()
    val endpoint = config.getString("my-dynamodb-journal.endpoint")
    val tableName = config.getString("my-dynamodb-journal.journal-table")
    val accessKey = config.getString("my-dynamodb-journal.aws-access-key-id")
    val secretKey = config.getString("my-dynamodb-journal.aws-secret-access-key")

    val client: DynamoDbClient = DynamoDbClient.builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
      .endpointOverride(URI.create(endpoint))
      .build()

    val persistenceId = "journal-P-OldFormatEvents-0"

    val messagePayloads = Seq(
      "ChEIARINrO0ABXQABmEtMDAwMRABGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwMhACGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwMxADGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNBAEGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNRAFGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNhAGGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNxAHGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwOBAIGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwORAJGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMBAKGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMRALGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMhAMGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMxANGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNBAOGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNRAPGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNhAQGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNxARGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxOBASGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxORATGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAyMBAUGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==")

    def createItem(number: Int, data: String): Unit = {
      val item = Map(
        "par" -> AttributeValue.fromS(persistenceId),
        "num" -> AttributeValue.fromN(number.toString),
        "pay" -> AttributeValue.fromB(SdkBytes.fromByteArray(Base64.getDecoder.decode(data)))).asJava
      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    }

    for {
      i <- messagePayloads.indices
      payload = messagePayloads(i)
    } yield createItem(i + 1, payload)

    client.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
    loadV1VersionData()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dynamo.shutdown()
    system.terminate().futureValue
  }

  override val persistenceId = "OldFormatEvents"
  lazy val journal = Persistence(system).journalFor("")

  "DynamoDB Journal (Backwards Compatibility Test)" must {

    val messages = 20
    val probe = TestProbe()

    "successfully replay events in old format - created by old version of the plugin" in {

      journal ! ReplayMessages(0, 20, Long.MaxValue, persistenceId, probe.ref)
      (1 to messages).foreach(i => {
        val msg = probe.expectMsgType[ReplayedMessage]
        msg.persistent.sequenceNr.toInt should ===(i)
        msg.persistent.payload should ===(f"a-$i%04d")
      })
      probe.expectMsg(RecoverySuccess(messages))
    }

  }

}
