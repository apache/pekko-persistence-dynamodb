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

package org.apache.pekko.persistence.dynamodb.journal

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ Collections, Map => JMap }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.core.exception.{ ApiCallAttemptTimeoutException, SdkClientException }
import software.amazon.awssdk.services.dynamodb.model._

import org.apache.pekko
import pekko.actor.ActorSystem
import pekko.persistence.dynamodb.{ Item, N, S }
import pekko.testkit.TestKit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class DynamoDBRetrySpec extends TestKit(ActorSystem("DynamoDBRetrySpec")) with AnyWordSpecLike with Matchers
    with BeforeAndAfterAll {
  import DynamoDBRecovery.collectUnprocessedItems

  implicit val ec: ExecutionContext = system.dispatcher

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val Table = "journal"

  private def item(n: Int): Item = Map(Key -> S("p"), Sort -> N(n)).asJava

  private def keys(ns: Int*): KeysAndAttributes = KeysAndAttributes.builder().keys(ns.map(item): _*).build()

  private def response(items: Seq[Int], unprocessed: Seq[Int]): BatchGetItemResponse = {
    val b = BatchGetItemResponse.builder().responses(Collections.singletonMap(Table, items.map(item).asJava))
    if (unprocessed.nonEmpty) b.unprocessedKeys(Collections.singletonMap(Table, keys(unprocessed: _*)))
    b.build()
  }

  private def itemsOf(r: BatchGetItemResponse): Seq[Int] =
    r.responses.get(Table).asScala.map(_.get(Sort).n.toInt).toSeq

  "collectUnprocessedItems" should {

    "return a fully processed response as it is" in {
      val first = response(Seq(1, 2), Nil)
      val result = collectUnprocessedItems(Table, first, _ => fail("should not fetch"), system.scheduler)
      Await.result(result, 3.seconds) should be theSameInstanceAs first
    }

    "re-request the unprocessed keys with backoff and merge the items of every round" in {
      val requested = new java.util.concurrent.ConcurrentLinkedQueue[Seq[Int]]
      val rounds = Iterator(response(Seq(3), Seq(4)), response(Seq(4), Nil))
      def fetch(req: JMap[String, KeysAndAttributes]): Future[BatchGetItemResponse] = {
        requested.add(req.get(Table).keys.asScala.map(_.get(Sort).n.toInt).toSeq)
        Future.successful(rounds.next())
      }

      val start = System.nanoTime()
      val result = Await.result(
        collectUnprocessedItems(Table, response(Seq(1, 2), Seq(3, 4)), fetch, system.scheduler,
          initialBackoff = 100.millis),
        3.seconds)
      val elapsed = (System.nanoTime() - start).nanos

      itemsOf(result) shouldBe Seq(1, 2, 3, 4)
      result.unprocessedKeys.isEmpty shouldBe true
      requested.asScala.toSeq shouldBe Seq(Seq(3, 4), Seq(4))
      // 100ms before the first retry, 200ms before the second
      elapsed should be >= 300.millis
    }

    "fail once the retries are exhausted" in {
      val calls = new AtomicInteger
      val result = collectUnprocessedItems(
        Table,
        response(Nil, Seq(1)),
        _ => { calls.incrementAndGet(); Future.successful(response(Nil, Seq(1))) },
        system.scheduler,
        retries = 3)
      val e = the[DynamoDBJournalFailure] thrownBy Await.result(result, 3.seconds)
      e.getMessage should include("after 3 tries")
      calls.get shouldBe 3
    }
  }

  "DynamoRetriableException" should {

    def retriable(ex: Throwable): Boolean = DynamoRetriableException.unapply(ex).isDefined

    "retry throttling and server errors" in {
      retriable(ProvisionedThroughputExceededException.builder().message("slow down").build()) shouldBe true
      retriable(InternalServerErrorException.builder().message("oops").build()) shouldBe true
      retriable(DynamoDbException.builder().statusCode(503).message("unavailable").build()) shouldBe true
    }

    "retry the transient client failures the AWS SDK no longer retries itself" in {
      retriable(SdkClientException.create("connection reset", new IOException("reset"))) shouldBe true
      retriable(ApiCallAttemptTimeoutException.create(1000L)) shouldBe true
    }

    "not retry client errors" in {
      retriable(ConditionalCheckFailedException.builder().message("no").build()) shouldBe false
      retriable(DynamoDbException.builder().statusCode(400).message("bad request").build()) shouldBe false
      retriable(SdkClientException.create("bad config")) shouldBe false
      retriable(new IllegalStateException("no")) shouldBe false
    }
  }
}
