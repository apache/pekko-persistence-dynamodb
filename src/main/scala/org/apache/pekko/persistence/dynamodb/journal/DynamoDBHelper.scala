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

import com.amazonaws.{ AmazonServiceException, AmazonWebServiceRequest }
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import org.apache.pekko
import pekko.actor.{ ActorRef, Scheduler }
import pekko.event.LoggingAdapter
import pekko.pattern.after
import pekko.persistence.dynamodb.{ DynamoDBConfig, Item }
import pekko.util.ccompat.JavaConverters._
import pekko.annotation.InternalApi

import java.util.{ concurrent => juc }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._

case class LatencyReport(nanos: Long, retries: Int)
private class RetryStateHolder(var retries: Int = 10, var backoff: FiniteDuration = 1.millis)

/**
 * Auxiliary object to help determining whether we should retry on a certain throwable.
 */
@InternalApi
private object DynamoRetriableException {
  def unapply(ex: AmazonServiceException) = {
    ex match {
      // 50x network glitches
      case _: InternalServerErrorException =>
        Some(ex)
      case ase if ase.getStatusCode >= 502 && ase.getStatusCode <= 504 =>
        // retry on more common server errors
        Some(ex)

      // 400 throughput issues
      case _: ProvisionedThroughputExceededException =>
        Some(ex)
      case _: RequestLimitExceededException =>
        // rate of on-demand requests exceeds the allowed account throughput
        // and the table cannot be scaled further
        Some(ex)
      case ase if ase.getErrorCode == "ThrottlingException" =>
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
  val dynamoDB: AmazonDynamoDBAsyncClient
  val log: LoggingAdapter
  val settings: DynamoDBConfig
  import settings._

  def shutdown(): Unit = dynamoDB.shutdown()

  private var reporter: ActorRef = _
  def setReporter(ref: ActorRef): Unit = reporter = ref

  private def send[In <: AmazonWebServiceRequest, Out](aws: In, func: AsyncHandler[In, Out] => juc.Future[Out])(implicit
      d: Describe[_ >: In]): Future[Out] = {

    def name = d.desc(aws)

    def sendSingle(): Future[Out] = {
      val p = Promise[Out]()

      val handler = new AsyncHandler[In, Out] {
        override def onError(ex: Exception) =
          ex match {
            case DynamoRetriableException(_) =>
              p.tryFailure(ex)
            case _ =>
              val n = name
              log.error(ex, "failure while executing {}", n)
              p.tryFailure(new DynamoDBJournalFailure("failure while executing " + n, ex))
          }
        override def onSuccess(req: In, resp: Out) = p.trySuccess(resp)
      }

      try {
        func(handler)
      } catch {
        case ex: Throwable =>
          log.error(ex, "failure while preparing {}", name)
          p.tryFailure(ex)
      }

      p.future
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
      case other =>
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

  trait Describe[T] {
    def desc(t: T): String
    protected def formatKey(i: Item): String = {
      val key = i.get(Key) match {
        case null => "<none>"
        case x    => x.getS
      }
      val sort = i.get(Sort) match {
        case null => "<none>"
        case x    => x.getN
      }
      s"[$Key=$key,$Sort=$sort]"
    }
  }

  object Describe {
    implicit object GenericDescribe extends Describe[AmazonWebServiceRequest] {
      def desc(aws: AmazonWebServiceRequest): String = aws.getClass.getSimpleName
    }
  }

  implicit object DescribeDescribe extends Describe[DescribeTableRequest] {
    def desc(aws: DescribeTableRequest): String = s"DescribeTableRequest(${aws.getTableName})"
  }

  implicit object QueryDescribe extends Describe[QueryRequest] {
    def desc(aws: QueryRequest): String = s"QueryRequest(${aws.getTableName},${aws.getExpressionAttributeValues})"
  }

  implicit object PutItemDescribe extends Describe[PutItemRequest] {
    def desc(aws: PutItemRequest): String = s"PutItemRequest(${aws.getTableName},${formatKey(aws.getItem)})"
  }

  implicit object DeleteDescribe extends Describe[DeleteItemRequest] {
    def desc(aws: DeleteItemRequest): String = s"DeleteItemRequest(${aws.getTableName},${formatKey(aws.getKey)})"
  }

  implicit object BatchGetItemDescribe extends Describe[BatchGetItemRequest] {
    def desc(aws: BatchGetItemRequest): String = {
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.getKeys.asScala.map(formatKey)
      s"BatchGetItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  implicit object BatchWriteItemDescribe extends Describe[BatchWriteItemRequest] {
    def desc(aws: BatchWriteItemRequest): String = {
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.asScala.map { write =>
        write.getDeleteRequest match {
          case null => "put" + formatKey(write.getPutRequest.getItem)
          case del  => "del" + formatKey(del.getKey)
        }
      }
      s"BatchWriteItemRequest($table, ${keys.mkString("(", ",", ")")})"
    }
  }

  def listTables(aws: ListTablesRequest): Future[ListTablesResult] =
    send[ListTablesRequest, ListTablesResult](aws, dynamoDB.listTablesAsync(aws, _))

  def describeTable(aws: DescribeTableRequest): Future[DescribeTableResult] =
    send[DescribeTableRequest, DescribeTableResult](aws, dynamoDB.describeTableAsync(aws, _))

  def createTable(aws: CreateTableRequest): Future[CreateTableResult] =
    send[CreateTableRequest, CreateTableResult](aws, dynamoDB.createTableAsync(aws, _))

  def updateTable(aws: UpdateTableRequest): Future[UpdateTableResult] =
    send[UpdateTableRequest, UpdateTableResult](aws, dynamoDB.updateTableAsync(aws, _))

  def deleteTable(aws: DeleteTableRequest): Future[DeleteTableResult] =
    send[DeleteTableRequest, DeleteTableResult](aws, dynamoDB.deleteTableAsync(aws, _))

  def query(aws: QueryRequest): Future[QueryResult] =
    send[QueryRequest, QueryResult](aws, dynamoDB.queryAsync(aws, _))

  def scan(aws: ScanRequest): Future[ScanResult] =
    send[ScanRequest, ScanResult](aws, dynamoDB.scanAsync(aws, _))

  def putItem(aws: PutItemRequest): Future[PutItemResult] =
    send[PutItemRequest, PutItemResult](aws, dynamoDB.putItemAsync(aws, _))

  def getItem(aws: GetItemRequest): Future[GetItemResult] =
    send[GetItemRequest, GetItemResult](aws, dynamoDB.getItemAsync(aws, _))

  def updateItem(aws: UpdateItemRequest): Future[UpdateItemResult] =
    send[UpdateItemRequest, UpdateItemResult](aws, dynamoDB.updateItemAsync(aws, _))

  def deleteItem(aws: DeleteItemRequest): Future[DeleteItemResult] =
    send[DeleteItemRequest, DeleteItemResult](aws, dynamoDB.deleteItemAsync(aws, _))

  def batchWriteItem(aws: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    send[BatchWriteItemRequest, BatchWriteItemResult](aws, dynamoDB.batchWriteItemAsync(aws, _))

  def batchGetItem(aws: BatchGetItemRequest): Future[BatchGetItemResult] =
    send[BatchGetItemRequest, BatchGetItemResult](aws, dynamoDB.batchGetItemAsync(aws, _))

}
