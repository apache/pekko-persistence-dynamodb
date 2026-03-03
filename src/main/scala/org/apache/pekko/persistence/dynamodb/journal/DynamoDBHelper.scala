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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import org.apache.pekko
import pekko.actor.{ ActorRef, Scheduler }
import pekko.annotation.InternalApi
import pekko.event.LoggingAdapter
import pekko.pattern.after
import pekko.persistence.dynamodb.{ DynamoDBConfig, Item }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

case class LatencyReport(nanos: Long, retries: Int)
private class RetryStateHolder(var retries: Int = 10, var backoff: FiniteDuration = 1.millis)

/**
 * Auxiliary object to help determining whether we should retry on a certain throwable.
 */
@InternalApi
private object DynamoRetriableException {
  def unapply(ex: DynamoDbException) = {
    ex match {
      // 50x network glitches
      case _: InternalServerErrorException =>
        Some(ex)
      case dbe if dbe.statusCode() >= 502 && dbe.statusCode() <= 504 =>
        // retry on more common server errors
        Some(ex)

      // 400 throughput issues
      case _: ProvisionedThroughputExceededException =>
        Some(ex)
      case _: RequestLimitExceededException =>
        // rate of on-demand requests exceeds the allowed account throughput
        // and the table cannot be scaled further
        Some(ex)
      case dbe if dbe.awsErrorDetails != null && dbe.awsErrorDetails.errorCode == "ThrottlingException" =>
        // rate of AWS requests exceeds the allowed throughput
        Some(ex)
      case _ =>
        None
    }
  }
}

trait DynamoDBHelper {

  implicit val ec: ExecutionContext
  val scheduler: Scheduler
  val dynamoDB: DynamoDbAsyncClient
  val log: LoggingAdapter
  val settings: DynamoDBConfig
  import settings._

  def shutdown(): Unit = dynamoDB.close()

  private var reporter: ActorRef = _
  def setReporter(ref: ActorRef): Unit = reporter = ref

  private def send[Out](name: => String, call: => Future[Out]): Future[Out] = {

    def sendSingle(): Future[Out] = {
      call.recoverWith {
        case ex: DynamoDbException if DynamoRetriableException.unapply(ex).isEmpty =>
          val n = name
          log.error(ex, "failure while executing {}", n)
          Future.failed(new DynamoDBJournalFailure("failure while executing " + n, ex))
        case ex: DynamoDbException =>
          Future.failed(ex)
      }
    }

    val state = new RetryStateHolder

    lazy val retry: PartialFunction[Throwable, Future[Out]] = {
      case DynamoRetriableException(ex) if state.retries > 0 =>
        val backoff = state.backoff
        state.retries -= 1
        state.backoff *= 2
        log.warning("failure while executing {} but will retry! Message: {}", name, ex.getMessage())
        after(backoff, scheduler)(sendSingle().recoverWith(retry))
      case other: DynamoDBJournalFailure => Future.failed(other)
      case other                         =>
        val n = name
        Future.failed(new DynamoDBJournalFailure("failed retry " + n, other))
    }

    if (Tracing) log.debug("{}", name)
    val start = if (reporter ne null) System.nanoTime else 0L

    // backoff retries when sending too fast
    val f = sendSingle().recoverWith(retry)

    if (reporter ne null) f.onComplete(_ => reporter ! LatencyReport(System.nanoTime - start, 10 - state.retries))

    f
  }

  protected def formatKey(i: Item): String = {
    val key = i.get(Key) match {
      case null => "<none>"
      case x    => x.s
    }
    val sort = i.get(Sort) match {
      case null => "<none>"
      case x    => x.n
    }
    s"[$Key=$key,$Sort=$sort]"
  }

  def listTables(aws: ListTablesRequest): Future[ListTablesResponse] =
    send(s"ListTablesRequest", dynamoDB.listTables(aws).asScala)

  def describeTable(aws: DescribeTableRequest): Future[DescribeTableResponse] =
    send(s"DescribeTableRequest(${aws.tableName})", dynamoDB.describeTable(aws).asScala)

  def createTable(aws: CreateTableRequest): Future[CreateTableResponse] =
    send(s"CreateTableRequest(${aws.tableName})", dynamoDB.createTable(aws).asScala)

  def updateTable(aws: UpdateTableRequest): Future[UpdateTableResponse] =
    send(s"UpdateTableRequest(${aws.tableName})", dynamoDB.updateTable(aws).asScala)

  def deleteTable(aws: DeleteTableRequest): Future[DeleteTableResponse] =
    send(s"DeleteTableRequest(${aws.tableName})", dynamoDB.deleteTable(aws).asScala)

  def query(aws: QueryRequest): Future[QueryResponse] =
    send(s"QueryRequest(${aws.tableName},${aws.expressionAttributeValues})", dynamoDB.query(aws).asScala)

  def scan(aws: ScanRequest): Future[ScanResponse] =
    send(s"ScanRequest(${aws.tableName})", dynamoDB.scan(aws).asScala)

  def putItem(aws: PutItemRequest): Future[PutItemResponse] =
    send(s"PutItemRequest(${aws.tableName},${formatKey(aws.item)})", dynamoDB.putItem(aws).asScala)

  def getItem(aws: GetItemRequest): Future[GetItemResponse] =
    send(s"GetItemRequest(${aws.tableName})", dynamoDB.getItem(aws).asScala)

  def updateItem(aws: UpdateItemRequest): Future[UpdateItemResponse] =
    send(s"UpdateItemRequest(${aws.tableName})", dynamoDB.updateItem(aws).asScala)

  def deleteItem(aws: DeleteItemRequest): Future[DeleteItemResponse] =
    send(s"DeleteItemRequest(${aws.tableName},${formatKey(aws.key)})", dynamoDB.deleteItem(aws).asScala)

  def batchWriteItem(aws: BatchWriteItemRequest): Future[BatchWriteItemResponse] =
    send(s"BatchWriteItemRequest(${aws.requestItems.keySet.iterator.next})", dynamoDB.batchWriteItem(aws).asScala)

  def batchGetItem(aws: BatchGetItemRequest): Future[BatchGetItemResponse] = {
    val entry = aws.requestItems.entrySet.iterator.next()
    val table = entry.getKey
    val keys = entry.getValue.keys.asScala.map(formatKey)
    send(s"BatchGetItemRequest($table, ${keys.mkString("(", ",", ")")})", dynamoDB.batchGetItem(aws).asScala)
  }

}
