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
import java.util.{ Map => JMap }

import scala.collection.BuildFrom
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, AttributeValueUpdate }

import org.apache.pekko.actor.{ ActorSystem, Scheduler }
import org.apache.pekko.event.{ Logging, LoggingAdapter }
import org.apache.pekko.persistence.dynamodb.journal.DynamoDBHelper

package object dynamodb {
  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  def S(value: String): AttributeValue = AttributeValue.fromS(value)

  def N(value: Long): AttributeValue = AttributeValue.fromN(value.toString)
  def N(value: String): AttributeValue = AttributeValue.fromN(value)
  val Naught = N(0)

  def B(value: Array[Byte]): AttributeValue = AttributeValue.fromB(SdkBytes.fromByteArray(value))

  /**
   * Like [[B]] but without the defensive copy, for freshly serialized payloads that the caller never
   * touches again. Avoids allocating and copying every event and snapshot a second time.
   */
  private[dynamodb] def unsafeB(value: Array[Byte]): AttributeValue =
    AttributeValue.fromB(SdkBytes.fromByteArrayUnsafe(value))

  /**
   * A DynamoDB response only carries a `LastEvaluatedKey` when there are further pages to read.
   * The AWS SDK models an absent key as an empty auto-constructed map, but an explicitly empty
   * map is possible too, so both are treated as "no more pages".
   */
  private[dynamodb] def nonEmptyKey(key: Item): Option[Item] =
    if (key == null || key.isEmpty) None else Some(key)

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

  /**
   * Picks the region the client signs requests for. An explicitly configured region wins. Otherwise the
   * AWS SDK default region provider chain (`AWS_REGION`, `aws.region`, the shared config file, instance
   * metadata) is consulted, and only when that finds nothing and a custom endpoint is configured - for
   * example DynamoDB Local, where the region does not matter - does it fall back to us-east-1.
   */
  private[dynamodb] def resolveRegion(
      configured: String,
      endpoint: String,
      defaultRegion: () => Option[Region],
      log: LoggingAdapter): Region =
    if (configured.nonEmpty) {
      // Region.of only rejects blank input - it happily accepts any other string - so an unknown
      // identifier is reported here rather than surfacing later as an obscure signing or DNS failure.
      val region =
        try Region.of(configured)
        catch {
          case e: IllegalArgumentException =>
            throw new IllegalArgumentException(
              s"Invalid AWS region '$configured' in configuration. " +
              "See https://docs.aws.amazon.com/general/latest/gr/rande.html for valid region identifiers.",
              e)
        }
      if (!Region.regions.contains(region)) {
        // not fatal: a region added after this AWS SDK release will not be in the built-in list
        log.warning(
          "AWS region '{}' is not known to this version of the AWS SDK - check for a typo. " +
          "See https://docs.aws.amazon.com/general/latest/gr/rande.html for valid region identifiers.",
          configured)
      }
      region
    } else
      defaultRegion() match {
        case Some(region)              => region
        case None if endpoint.nonEmpty =>
          log.debug("No AWS region configured or found by the AWS SDK, using us-east-1 for endpoint {}", endpoint)
          Region.US_EAST_1
        case None =>
          throw new IllegalArgumentException(
            "No AWS region is configured and the AWS SDK could not determine one. Set the `region` setting " +
            "of the plugin, or the AWS_REGION environment variable or aws.region system property. " +
            "See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html")
      }

  private def sdkDefaultRegion(): Option[Region] =
    try Option(DefaultAwsRegionProviderChain.builder().build().getRegion)
    catch { case _: SdkClientException => None }

  def dynamoClient(system: ActorSystem, settings: DynamoDBConfig): DynamoDBHelper = {
    val log = Logging(system, "DynamoDBClient")
    val builder = DynamoDbAsyncClient.builder()

    if (settings.AwsKey.nonEmpty && settings.AwsSecret.nonEmpty) {
      val creds = AwsBasicCredentials.create(settings.AwsKey, settings.AwsSecret)
      builder.credentialsProvider(StaticCredentialsProvider.create(creds))
    }

    builder.region(resolveRegion(settings.Region, settings.Endpoint, () => sdkDefaultRegion(), log))

    if (settings.Endpoint.nonEmpty) {
      builder.endpointOverride(URI.create(settings.Endpoint))
    }

    settings.client.ignoredSettings.foreach { key =>
      log.warning("aws-client-config.{} has no equivalent in the AWS SDK for Java v2 and is ignored", key)
    }
    builder.httpClientBuilder(settings.client.configure(NettyNioAsyncHttpClient.builder()))
    builder.overrideConfiguration(settings.client.configure(ClientOverrideConfiguration.builder()).build())

    val client = builder.build()
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(
        override val ec: ExecutionContext,
        override val dynamoDB: DynamoDbAsyncClient,
        override val settings: DynamoDBConfig,
        override val scheduler: Scheduler,
        override val log: LoggingAdapter)
        extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, system.scheduler, log)
  }

}
