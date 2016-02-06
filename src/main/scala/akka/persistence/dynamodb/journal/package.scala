/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.{ Logging, LoggingAdapter }
import java.util.{ Map => JMap }
import scala.concurrent._
import scala.util.Try
import java.util.concurrent.{ ThreadPoolExecutor, LinkedBlockingQueue, TimeUnit }
import scala.collection.generic.CanBuildFrom

package object journal {

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  // field names
  val Key = "key"
  val Payload = "payload"
  val SequenceNr = "sequenceNr"

  val KeyPayloadOverhead = 26 // including 16 bytes fudge factor

  import collection.JavaConverters._

  val schema = Seq(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(Key)).asJava
  val schemaAttributes = Seq(new AttributeDefinition().withAttributeName(Key).withAttributeType("S")).asJava

  def lift[T](f: Future[T]): Future[Try[T]] = {
    val p = Promise[Try[T]]
    f.onComplete(p.success)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def trySequence[A, M[X] <: TraversableOnce[X]](in: M[Future[A]])(implicit
    cbf: CanBuildFrom[M[Future[A]], Try[A], M[Try[A]]],
    executor: ExecutionContext): Future[M[Try[A]]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      val fb = lift(a)
      for (r <- fr; b <- fb) yield (r += b)
    }.map(_.result())

  def dynamoClient(system: ActorSystem, settings: DynamoDBJournalConfig): DynamoDBHelper = {
    val creds = new BasicAWSCredentials(settings.AwsKey, settings.AwsSecret)
    val conns = settings.client.config.getMaxConnections
    val executor = new ThreadPoolExecutor(Math.min(8, conns), conns, 5, TimeUnit.SECONDS, new LinkedBlockingQueue)
    executor.prestartAllCoreThreads()
    val client = new AmazonDynamoDBAsyncClient(creds, settings.client.config, executor)
    client.setEndpoint(settings.Endpoint)
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(
      override val ec: ExecutionContext,
      override val dynamoDB: AmazonDynamoDBAsyncClient,
      override val settings: DynamoDBJournalConfig,
      override val scheduler: Scheduler,
      override val log: LoggingAdapter
    ) extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, system.scheduler, Logging(system, "DynamoDBClient"))
  }
}
