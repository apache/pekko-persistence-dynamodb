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

package org.apache.pekko.persistence.dynamodb.snapshot

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit._
import org.apache.pekko.persistence._
import org.apache.pekko.persistence.SnapshotProtocol._
import org.apache.pekko.persistence.dynamodb.IntegSpec
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class SnapshotTooBigSpec extends TestKit(ActorSystem("SnapshotTooBigSpec"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with IntegSpec
    with DynamoDBUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    this.ensureSnapshotTableExists()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    senderProbe = TestProbe()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
  }

  private var senderProbe: TestProbe = _
  val persistenceId = "SnapshotTooBigSpec"
  val snapshotStore = Persistence(system).snapshotStoreFor("")

  "DynamoDB snapshot too big spec" must {

    "1 reject a snapshot that is over 400 KB compressed." in {
      // Expect 1 MB of random data to exceed 400KB compressed.
      val bytes = new Array[Byte](1 << 20)
      scala.util.Random.nextBytes(bytes)
      val metadata = SnapshotMetadata.apply(persistenceId, 1, 0)
      snapshotStore.tell(SaveSnapshot(metadata, bytes), senderProbe.ref)
      val rej = senderProbe.expectMsgType[SaveSnapshotFailure]
      rej.cause shouldBe a[DynamoDBSnapshotRejection]
      rej.cause.getMessage().startsWith("MaxItemSize exceeded") shouldBe true
    }
  }
}
