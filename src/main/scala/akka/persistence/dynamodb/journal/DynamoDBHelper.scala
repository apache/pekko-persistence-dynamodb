/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.reflect.ClassTag
import akka.event.LoggingAdapter
import scala.collection.JavaConverters._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import java.util.{ concurrent => juc }
import scala.util.control.NoStackTrace
import akka.pattern.after
import scala.concurrent.duration._
import akka.actor.Scheduler

case class BackoffException(retries: Int, orig: ProvisionedThroughputExceededException) extends AmazonServiceException("BackOff") with NoStackTrace

trait DynamoDBHelper {

  implicit val ec: ExecutionContext
  val scheduler: Scheduler
  val dynamoDB: AmazonDynamoDBAsyncClient
  val log: LoggingAdapter
  val settings: DynamoDBJournalConfig
  import settings._

  private def send[In <: AmazonWebServiceRequest, Out](aws: In,
                                                       func: AsyncHandler[In, Out] => juc.Future[Out],
                                                       retries: Int = 10,
                                                       backoff: FiniteDuration = 1.millis)(implicit d: Describe[_ >: In]): Future[AWSTry[Out]] = {
    val name = d.desc(aws)
    if (Tracing) log.debug("{} {}", name, aws)

    val p = Promise[AWSTry[Out]]

    val handler = new AsyncHandler[In, Out] {
      override def onError(ex: Exception) = ex match {
        case e: ProvisionedThroughputExceededException =>
          p.trySuccess(Left(BackoffException(retries, e)))
        case ase: AmazonServiceException =>
          log.error(ase, "failure while executing {}", name)
          p.trySuccess(Left(ase))
        case _ =>
          p.tryFailure(ex)
      }
      override def onSuccess(req: In, resp: Out) = p.trySuccess(Right(resp))
    }

    try {
      func(handler)
    } catch {
      case ex: Throwable =>
        log.error(ex, "failure while preparing {}", name)
        p.tryFailure(ex)
    }

    // backoff retries when sending too fast
    p.future.flatMap {
      case Left(BackoffException(x, _)) if x > 0 =>
        after(backoff, scheduler)(send(aws, func, retries - 1, backoff * 2))
      case Left(BackoffException(_, orig)) =>
        log.error(orig, "maximum backoff exceeded while executing {}", name)
        Future.successful(Left(orig))
      case other => Future.successful(other)
    }
  }

  trait Describe[T] {
    def desc(t: T): String
  }

  object Describe {
    implicit object GenericDescribe extends Describe[AmazonWebServiceRequest] {
      def desc(aws: AmazonWebServiceRequest): String = aws.getClass.getSimpleName
    }
  }

  implicit object DescribeDescribe extends Describe[DescribeTableRequest] {
    def desc(aws: DescribeTableRequest): String = s"DescribeTableRequest(${aws.getTableName})"
  }

  def listTables(aws: ListTablesRequest): Future[AWSTry[ListTablesResult]] =
    send[ListTablesRequest, ListTablesResult](aws, dynamoDB.listTablesAsync(aws, _))

  def describeTable(aws: DescribeTableRequest): Future[AWSTry[DescribeTableResult]] =
    send[DescribeTableRequest, DescribeTableResult](aws, dynamoDB.describeTableAsync(aws, _))

  def createTable(aws: CreateTableRequest): Future[AWSTry[CreateTableResult]] =
    send[CreateTableRequest, CreateTableResult](aws, dynamoDB.createTableAsync(aws, _))

  def updateTable(aws: UpdateTableRequest): Future[AWSTry[UpdateTableResult]] =
    send[UpdateTableRequest, UpdateTableResult](aws, dynamoDB.updateTableAsync(aws, _))

  def deleteTable(aws: DeleteTableRequest): Future[AWSTry[DeleteTableResult]] =
    send[DeleteTableRequest, DeleteTableResult](aws, dynamoDB.deleteTableAsync(aws, _))

  def query(aws: QueryRequest): Future[AWSTry[QueryResult]] =
    send[QueryRequest, QueryResult](aws, dynamoDB.queryAsync(aws, _))

  def scan(aws: ScanRequest): Future[AWSTry[ScanResult]] =
    send[ScanRequest, ScanResult](aws, dynamoDB.scanAsync(aws, _))

  def putItem(aws: PutItemRequest): Future[AWSTry[PutItemResult]] =
    send[PutItemRequest, PutItemResult](aws, dynamoDB.putItemAsync(aws, _))

  def getItem(aws: GetItemRequest): Future[AWSTry[GetItemResult]] =
    send[GetItemRequest, GetItemResult](aws, dynamoDB.getItemAsync(aws, _))

  def updateItem(aws: UpdateItemRequest): Future[AWSTry[UpdateItemResult]] =
    send[UpdateItemRequest, UpdateItemResult](aws, dynamoDB.updateItemAsync(aws, _))

  def deleteItem(aws: DeleteItemRequest): Future[AWSTry[DeleteItemResult]] =
    send[DeleteItemRequest, DeleteItemResult](aws, dynamoDB.deleteItemAsync(aws, _))

  def batchWriteItem(aws: BatchWriteItemRequest): Future[AWSTry[BatchWriteItemResult]] =
    send[BatchWriteItemRequest, BatchWriteItemResult](aws, dynamoDB.batchWriteItemAsync(aws, _))

  def batchGetItem(aws: BatchGetItemRequest): Future[AWSTry[BatchGetItemResult]] =
    send[BatchGetItemRequest, BatchGetItemResult](aws, dynamoDB.batchGetItemAsync(aws, _))

}
