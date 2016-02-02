/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
import akka.event.LoggingAdapter
import scala.collection.JavaConverters._

trait DynamoDBHelper {

  implicit val ec: ExecutionContext

  val dynamoDB: AmazonDynamoDBClient

  val tracing: Boolean

  val log: LoggingAdapter

  def toRight[T](t: T) = Right(t)

  def fold[T](fe: Future[Either[AmazonServiceException, T]]): Future[T] = fe.map(_.fold(e => throw e, t => t))

  def execute[T](f: => T)(implicit tag: ClassTag[T]) = Future(f).mapTo[T].map(toRight)

  def sendListTables(aws: ListTablesRequest): Future[ListTablesResult] = fold(listTables(aws))

  def listTables(aws: ListTablesRequest): Future[Either[AmazonServiceException, ListTablesResult]] =
    execute {
      if (tracing) log.debug("ListTablesRequest {}", aws)
      try dynamoDB.listTables(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during ListTablesRequest {}", aws)
          throw ex
      }
    }

  def sendQuery(aws: QueryRequest): Future[QueryResult] = fold(query(aws))

  def query(aws: QueryRequest): Future[Either[AmazonServiceException, QueryResult]] =
    execute {
      if (tracing) log.debug("QueryRequest {}", aws)
      try dynamoDB.query(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during QueryRequest {}", aws)
          throw ex
      }
    }

  def sendScan(aws: ScanRequest): Future[ScanResult] = fold(scan(aws))

  def scan(aws: ScanRequest): Future[Either[AmazonServiceException, ScanResult]] =
    execute {
      if (tracing) log.debug("ScanRequest {}", aws)
      try dynamoDB.scan(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during ScanRequest {}", aws)
          throw ex
      }
    }

  def sendUpdateItem(aws: UpdateItemRequest): Future[UpdateItemResult] = fold(updateItem(aws))

  def updateItem(aws: UpdateItemRequest): Future[Either[AmazonServiceException, UpdateItemResult]] =
    execute {
      if (tracing) log.debug("UpdateItemRequest {}", aws)
      try dynamoDB.updateItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during UpdateItemRequest {}", aws)
          throw ex
      }
    }

  def sendPutItem(aws: PutItemRequest): Future[PutItemResult] = fold(putItem(aws))

  def putItem(aws: PutItemRequest): Future[Either[AmazonServiceException, PutItemResult]] =
    execute {
      if (tracing) log.debug("PutItemRequest {}", aws)
      try dynamoDB.putItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during PutItemRequest {}", aws)
          throw ex
      }
    }

  def sendDescribeTable(aws: DescribeTableRequest): Future[DescribeTableResult] = fold(describeTable(aws))

  def describeTable(aws: DescribeTableRequest): Future[Either[AmazonServiceException, DescribeTableResult]] =
    execute {
      if (tracing) log.debug("DescribeTableRequest {}", aws)
      try dynamoDB.describeTable(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during DescribeTableRequest {}", aws)
          throw ex
      }
    }

  def sendCreateTable(aws: CreateTableRequest): Future[CreateTableResult] = fold(createTable(aws))

  def createTable(aws: CreateTableRequest): Future[Either[AmazonServiceException, CreateTableResult]] =
    execute {
      if (tracing) log.debug("CreateTableRequest {}", aws)
      try dynamoDB.createTable(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during CreateTableRequest {}", aws)
          throw ex
      }
    }

  def sendUpdateTable(aws: UpdateTableRequest): Future[UpdateTableResult] = fold(updateTable(aws))

  def updateTable(aws: UpdateTableRequest): Future[Either[AmazonServiceException, UpdateTableResult]] =
    execute {
      if (tracing) log.debug("UpdateTableRequest {}", aws)
      try dynamoDB.updateTable(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during UpdateTableRequest {}", aws)
          throw ex
      }
    }

  def sendDeleteTable(aws: DeleteTableRequest): Future[DeleteTableResult] = fold(deleteTable(aws))

  def deleteTable(aws: DeleteTableRequest): Future[Either[AmazonServiceException, DeleteTableResult]] =
    execute {
      if (tracing) log.debug("DeleteTableRequest {}", aws)
      try dynamoDB.deleteTable(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during DeleteTableRequest {}", aws)
          throw ex
      }
    }

  def sendGetItem(aws: GetItemRequest): Future[GetItemResult] = fold(getItem(aws))

  def getItem(aws: GetItemRequest): Future[Either[AmazonServiceException, GetItemResult]] =
    execute {
      if (tracing) log.debug("GetItemRequest {}", aws)
      try dynamoDB.getItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during GetItemRequest {}", aws)
          throw ex
      }
    }

  def sendBatchWriteItem(awsWrite: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    fold(batchWriteItem(awsWrite))

  def batchWriteItem(aws: BatchWriteItemRequest): Future[Either[AmazonServiceException, BatchWriteItemResult]] =
    execute {
      if (tracing && log.isDebugEnabled) log.debug("BatchWriteItemRequest {}", aws)
      try dynamoDB.batchWriteItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during BatchWriteItemRequest {}", aws)
          throw ex
      }
    }

  def sendBatchGetItem(awsGet: BatchGetItemRequest): Future[BatchGetItemResult] = fold(batchGetItem(awsGet))

  def batchGetItem(aws: BatchGetItemRequest): Future[Either[AmazonServiceException, BatchGetItemResult]] =
    execute {
      if (tracing && log.isDebugEnabled) {
        val req = aws.getRequestItems.asScala.mapValues { v => s"{Keys:{${v.getKeys.get(0)}... (${v.getKeys.size})}, AttributesToGet: ${v.getAttributesToGet}, ConsistentRead: ${v.getConsistentRead}}" }
        val summary = s"{RequestItems: $req, ReturnConsumedCapacity: ${aws.getReturnConsumedCapacity}}"
        log.debug("BatchGetItemRequest {}", summary)
      }
      try dynamoDB.batchGetItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during BatchGetItemRequest {}", aws)
          throw ex
      }
    }

  def sendDeleteItem(awsDel: DeleteItemRequest): Future[DeleteItemResult] = fold(deleteItem(awsDel))

  def deleteItem(aws: DeleteItemRequest): Future[Either[AmazonServiceException, DeleteItemResult]] =
    execute {
      if (tracing) log.debug("DeleteItemRequest {}", aws)
      try dynamoDB.deleteItem(aws)
      catch {
        case ex: Throwable =>
          log.error(ex, "failure during DeleteItemRequest {}", aws)
          throw ex
      }
    }

}
