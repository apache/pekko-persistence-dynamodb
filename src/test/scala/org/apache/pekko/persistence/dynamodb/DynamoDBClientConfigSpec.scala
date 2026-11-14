/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pekko.persistence.dynamodb

import java.time.{ Duration => JDuration }

import software.amazon.awssdk.core.client.config.{ ClientOverrideConfiguration, SdkAdvancedClientOption }
import software.amazon.awssdk.regions.Region

import org.apache.pekko
import pekko.event.NoLogging
import pekko.persistence.dynamodb.journal.DynamoDBJournalConfig
import pekko.persistence.dynamodb.query.DynamoDBReadJournalConfig
import pekko.persistence.dynamodb.snapshot.DynamoDBSnapshotConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.typesafe.config.{ Config, ConfigFactory }

class DynamoDBClientConfigSpec extends AnyWordSpec with Matchers {

  private def configWith(overrides: String): Config =
    ConfigFactory.parseString(overrides).withFallback(ConfigFactory.defaultReferenceUnresolved()).resolve()

  private def journalClient(overrides: String): DynamoDBClientConfig =
    new DynamoDBJournalConfig(configWith(overrides).getConfig("dynamodb-journal")).client

  "DynamoDBClientConfig" should {

    "leave every AWS SDK default in place with the reference configuration" in {
      val client = journalClient("")
      client.maxConnections shouldBe None
      client.socketTimeout shouldBe None
      client.maxErrorRetry shouldBe None
      client.proxyHost shouldBe None
      client.ignoredSettings shouldBe Nil
      client.toString shouldBe "{}"
    }

    "read the 1.x keys that have an AWS SDK v2 equivalent" in {
      val client = journalClient("""
        dynamodb-journal.aws-client-config {
          max-connections = 200
          connection-timeout = 1500
          socket-timeout = 3000
          client-execution-timeout = 10000
          request-timeout = 2000
          use-tcp-keepalive = true
          max-error-retry = 2
          proxy-host = "proxy.example.com"
          proxy-port = 3128
          proxy-password = "secret"
          user-agent = "my-app"
        }""")
      client.maxConnections shouldBe Some(200)
      client.connectionTimeout shouldBe Some(JDuration.ofMillis(1500))
      client.socketTimeout shouldBe Some(JDuration.ofMillis(3000))
      client.clientExecutionTimeout shouldBe Some(JDuration.ofSeconds(10))
      client.requestTimeout shouldBe Some(JDuration.ofSeconds(2))
      client.useTcpKeepAlive shouldBe Some(true)
      client.maxErrorRetry shouldBe Some(2)
      client.proxyHost shouldBe Some("proxy.example.com")
      client.proxyPort shouldBe Some(3128)
      client.userAgent shouldBe Some("my-app")
      client.ignoredSettings shouldBe Nil
    }

    "never show the proxy password" in {
      val client = journalClient("""dynamodb-journal.aws-client-config.proxy-password = "secret"""")
      client.toString should include("proxy-password:<redacted>")
      (client.toString should not).include("secret")
    }

    "report 1.x keys that have no AWS SDK v2 equivalent" in {
      val client = journalClient("""
        dynamodb-journal.aws-client-config {
          use-gzip = true
          socket-buffer-size-hints = [1024, 1024]
        }""")
      client.ignoredSettings should contain theSameElementsAs Seq("use-gzip", "socket-buffer-size-hints")
    }

    "disable the AWS SDK retries unless max-error-retry is set, so they do not multiply with the plugin's" in {
      journalClient("").configure(
        ClientOverrideConfiguration.builder()).build().retryStrategy().get.maxAttempts shouldBe 1

      val overrides =
        journalClient("""
          dynamodb-journal.aws-client-config {
            max-error-retry = 3
            client-execution-timeout = 10000
            request-timeout = 2000
            user-agent = "my-app"
          }""").configure(ClientOverrideConfiguration.builder()).build()
      overrides.retryStrategy().get.maxAttempts shouldBe 4
      overrides.apiCallTimeout().get shouldBe JDuration.ofSeconds(10)
      overrides.apiCallAttemptTimeout().get shouldBe JDuration.ofSeconds(2)
      overrides.advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX).get shouldBe "my-app"
    }

    "be inherited by the snapshot store and the read journal" in {
      val config = configWith("dynamodb-journal.aws-client-config.max-connections = 200")
      new DynamoDBSnapshotConfig(config.getConfig("dynamodb-snapshot-store")).client.maxConnections shouldBe Some(200)
      new DynamoDBReadJournalConfig(config.getConfig("dynamodb-read-journal")).client.maxConnections shouldBe Some(200)
    }

    "be shown with the region in the logged configuration" in {
      val config = configWith("""
        dynamodb-journal.region = "eu-west-1"
        dynamodb-journal.aws-client-config.max-connections = 200""")
      for (str <- Seq(
          new DynamoDBJournalConfig(config.getConfig("dynamodb-journal")).toString,
          new DynamoDBSnapshotConfig(config.getConfig("dynamodb-snapshot-store")).toString,
          new DynamoDBReadJournalConfig(config.getConfig("dynamodb-read-journal")).toString)) {
        str should include("Region:eu-west-1")
        str should include("max-connections:200")
      }
    }
  }

  "resolveRegion" should {
    val noRegion = () => None
    val sdkRegion = () => Some(Region.EU_WEST_1)

    "use the configured region" in {
      resolveRegion("ap-southeast-2", "", sdkRegion, NoLogging) shouldBe Region.AP_SOUTHEAST_2
    }

    "reject a blank-but-not-empty region" in {
      an[IllegalArgumentException] should be thrownBy resolveRegion(" ", "", sdkRegion, NoLogging)
    }

    "accept a region unknown to the AWS SDK, which may be newer than the SDK" in {
      resolveRegion("xx-future-1", "", noRegion, NoLogging) shouldBe Region.of("xx-future-1")
    }

    "use the AWS SDK default region when none is configured, also with a custom endpoint" in {
      resolveRegion("", "", sdkRegion, NoLogging) shouldBe Region.EU_WEST_1
      resolveRegion("", "https://dynamodb.eu-west-1.amazonaws.com", sdkRegion, NoLogging) shouldBe Region.EU_WEST_1
    }

    "fall back to us-east-1 for a custom endpoint when the AWS SDK finds no region" in {
      resolveRegion("", "http://localhost:8000", noRegion, NoLogging) shouldBe Region.US_EAST_1
    }

    "fail with a clear message when no region can be determined at all" in {
      val e = the[IllegalArgumentException] thrownBy resolveRegion("", "", noRegion, NoLogging)
      e.getMessage should include("No AWS region")
    }
  }
}
