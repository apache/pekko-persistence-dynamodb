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

package org.apache.pekko.persistence

import java.net.URI
import org.apache.pekko.actor.{ ActorSystem, Scheduler }
import org.apache.pekko.event.{ Logging, LoggingAdapter }
import org.apache.pekko.persistence.dynamodb.journal.DynamoDBHelper
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, AttributeValueUpdate }

import java.util.{ Map => JMap }
import scala.collection.BuildFrom
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

package object dynamodb {
  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  def S(value: String): AttributeValue = AttributeValue.fromS(value)

  def N(value: Long): AttributeValue = AttributeValue.fromN(value.toString)
  def N(value: String): AttributeValue = AttributeValue.fromN(value)
  val Naught = N(0)

  def B(value: Array[Byte]): AttributeValue = AttributeValue.fromB(SdkBytes.fromByteArray(value))

  def lift[T](f: Future[T]): Future[Try[T]] = {
    val p = Promise[Try[T]]()
    f.onComplete(p.success)(ExecutionContext.parasitic)
    p.future
  }

  def liftUnit(f: Future[Any]): Future[Try[Unit]] = {
    val p = Promise[Try[Unit]]()
    f.onComplete {
      case Success(_)     => p.success(Success(()))
      case f @ Failure(_) => p.success(f.asInstanceOf[Failure[Unit]])
    }(ExecutionContext.parasitic)
    p.future
  }

  def trySequence[A, M[X] <: IterableOnce[X]](in: M[Future[A]])(
      implicit
      cbf: BuildFrom[M[Future[A]], Try[A], M[Try[A]]],
      executor: ExecutionContext): Future[M[Try[A]]] =
    in.iterator.foldLeft(Future.successful(cbf.newBuilder(in))) { (fr, a) =>
      val fb = lift(a)
      for (r <- fr; b <- fb) yield r += b
    }.map(_.result())

  def dynamoClient(system: ActorSystem, settings: DynamoDBConfig): DynamoDBHelper = {
    val builder = DynamoDbAsyncClient.builder()

    if (settings.AwsKey.nonEmpty && settings.AwsSecret.nonEmpty) {
      val creds = AwsBasicCredentials.create(settings.AwsKey, settings.AwsSecret)
      builder.credentialsProvider(StaticCredentialsProvider.create(creds))
    }

    if (settings.Region.nonEmpty) {
      builder.region(Region.of(settings.Region))
    } else if (settings.Endpoint.nonEmpty) {
      // When using a custom endpoint (e.g. DynamoDB Local), SDK v2 requires a region for
      // request signing. Fall back to us-east-1 since the region is irrelevant for local endpoints.
      builder.region(Region.US_EAST_1)
    }

    if (settings.Endpoint.nonEmpty) {
      builder.endpointOverride(URI.create(settings.Endpoint))
    }

    val client = builder.build()
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(
        override val ec: ExecutionContext,
        override val dynamoDB: DynamoDbAsyncClient,
        override val settings: DynamoDBConfig,
        override val scheduler: Scheduler,
        override val log: LoggingAdapter)
        extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, system.scheduler, Logging(system, "DynamoDBClient"))
  }

}
