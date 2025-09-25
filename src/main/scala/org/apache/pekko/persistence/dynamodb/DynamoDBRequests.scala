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

package org.apache.pekko.persistence.dynamodb

import java.util.Collections
import java.util.{ List => JList, Map => JMap }
import com.amazonaws.services.dynamodbv2.model._
import org.apache.pekko
import pekko.Done
import pekko.actor.{ Actor, ActorLogging }
import pekko.pattern.after
import pekko.persistence.dynamodb.journal.DynamoDBHelper

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[dynamodb] trait DynamoDBRequests {
  this: ActorLogging with Actor =>

  val journalSettings: DynamoDBConfig
  def dynamo: DynamoDBHelper

  import context.dispatcher
  import journalSettings._

  def putItem(item: Item): PutItemRequest = new PutItemRequest().withTableName(Table).withItem(item)

  def batchWriteReq(writes: Seq[WriteRequest]): BatchWriteItemRequest =
    batchWriteReq(Collections.singletonMap(Table, writes.asJava))

  def batchWriteReq(items: JMap[String, JList[WriteRequest]]): BatchWriteItemRequest =
    new BatchWriteItemRequest().withRequestItems(items).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  /*
   * Request execution helpers.
   */

  /**
   * Execute the given WriteRequests in batches of MaxBatchWrite, ignoring and
   * logging all errors. The returned Future never fails.
   */
  def doBatch(desc: Seq[WriteRequest] => String, writes: Seq[WriteRequest]): Future[Done] =
    Future
      .sequence {
        writes.grouped(MaxBatchWrite).map { batch =>
          dynamo.batchWriteItem(batchWriteReq(batch)).flatMap(sendUnprocessedItems(_)).recover {
            case NonFatal(ex) => log.error(ex, "cannot " + desc(batch))
          }
        }
      }
      .map(_ => Done)

  /**
   * Sends the unprocessed batch write items, and sets the back-off.
   * if no more retries remain (number of back-off retries exhausted), we throw a Runtime exception
   *
   * Note: the DynamoDB client supports automatic retries, however a batch will not fail if some of the items in the
   * batch fail; that is why we need our own back-off mechanism here.  If we exhaust OUR retry logic on top of
   * the retries from the client, then we are hosed and cannot continue; that is why we have a RuntimeException here
   */
  private def sendUnprocessedItems(
      result: BatchWriteItemResult,
      retriesRemaining: Int = 10,
      backoff: FiniteDuration = 1.millis): Future[BatchWriteItemResult] = {
    val unprocessed: Int = result.getUnprocessedItems.get(Table) match {
      case null  => 0
      case items => items.size
    }
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write ${result.getUnprocessedItems.get(Table)} after 10 tries")
    } else {
      val rest = batchWriteReq(result.getUnprocessedItems)
      after(backoff, context.system.scheduler)(
        dynamo.batchWriteItem(rest).flatMap(r => sendUnprocessedItems(r, retriesRemaining - 1, backoff * 2)))
    }
  }

}
