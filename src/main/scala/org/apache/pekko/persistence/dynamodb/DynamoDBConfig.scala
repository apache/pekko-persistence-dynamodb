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

import java.time.{ Duration => JDuration }

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.{ ClientOverrideConfiguration, SdkAdvancedClientOption }
import software.amazon.awssdk.http.nio.netty.{ NettyNioAsyncHttpClient, ProxyConfiguration }

import com.typesafe.config.{ Config, ConfigFactory }

trait DynamoDBConfig {
  val AwsKey: String
  val AwsSecret: String
  val Endpoint: String
  val Region: String
  val ClientDispatcher: String
  val client: DynamoDBClientConfig
  val Tracing: Boolean
  val MaxBatchGet: Int
  val MaxBatchWrite: Int
  val MaxItemSize: Int
  val Table: String
  val JournalName: String

}

/**
 * The `aws-client-config` block. The keys are the ones used by the AWS SDK v1 `ClientConfiguration` in
 * version 1.x of this plugin; the ones that have an AWS SDK v2 equivalent are mapped onto the Netty async
 * HTTP client and the client override configuration. Every key defaults to `default`, which leaves the
 * AWS SDK default in place.
 */
final class DynamoDBClientConfig(c: Config) {
  private val cc: Config =
    if (c.hasPath("aws-client-config")) c.getConfig("aws-client-config") else ConfigFactory.empty

  private def isSet(path: String): Boolean =
    cc.hasPath(path) && cc.getValue(path).unwrapped != "default"

  private def opt[T](path: String)(extract: (Config, String) => T): Option[T] =
    if (isSet(path)) Some(extract(cc, path)) else None

  private def millis(path: String): Option[JDuration] = opt(path)((c, p) => JDuration.ofMillis(c.getLong(p)))

  val maxConnections: Option[Int] = opt("max-connections")(_.getInt(_))
  val connectionTimeout: Option[JDuration] = millis("connection-timeout")
  val socketTimeout: Option[JDuration] = millis("socket-timeout")
  val clientExecutionTimeout: Option[JDuration] = millis("client-execution-timeout")
  val requestTimeout: Option[JDuration] = millis("request-timeout")
  val connectionTtl: Option[JDuration] = millis("connection-ttl")
  val connectionMaxIdle: Option[JDuration] = millis("connection-max-idle-millis")
  val useReaper: Option[Boolean] = opt("use-reaper")(_.getBoolean(_))
  val useTcpKeepAlive: Option[Boolean] = opt("use-tcp-keepalive")(_.getBoolean(_))

  /**
   * The number of retries the AWS SDK makes itself, on top of the first attempt. When this is not set the
   * SDK does not retry at all, since the plugin retries throttling, server errors and transient I/O failures
   * with its own exponential backoff and the two would otherwise multiply.
   */
  val maxErrorRetry: Option[Int] = opt("max-error-retry")(_.getInt(_))
  val proxyHost: Option[String] = opt("proxy-host")(_.getString(_))
  val proxyPort: Option[Int] = opt("proxy-port")(_.getInt(_))
  val proxyUsername: Option[String] = opt("proxy-username")(_.getString(_))
  val proxyPassword: Option[String] = opt("proxy-password")(_.getString(_))
  val userAgent: Option[String] = opt("user-agent")(_.getString(_))

  /**
   * AWS SDK v1 settings that have no AWS SDK v2 equivalent, and are ignored even when they are set.
   */
  val ignoredSettings: List[String] = DynamoDBClientConfig.UnsupportedKeys.filter(isSet)

  private[dynamodb] def configure(http: NettyNioAsyncHttpClient.Builder): NettyNioAsyncHttpClient.Builder = {
    maxConnections.foreach(n => http.maxConcurrency(n))
    connectionTimeout.foreach(http.connectionTimeout)
    socketTimeout.foreach { d =>
      http.readTimeout(d)
      http.writeTimeout(d)
    }
    connectionTtl.foreach(http.connectionTimeToLive)
    connectionMaxIdle.foreach(http.connectionMaxIdleTime)
    useReaper.foreach(b => http.useIdleConnectionReaper(b))
    useTcpKeepAlive.foreach(b => http.tcpKeepAlive(b))
    proxyHost.foreach { host =>
      val proxy = ProxyConfiguration.builder().host(host)
      proxyPort.foreach(p => proxy.port(p))
      proxyUsername.foreach(proxy.username)
      proxyPassword.foreach(proxy.password)
      http.proxyConfiguration(proxy.build())
    }
    http
  }

  private[dynamodb] def configure(
      overrides: ClientOverrideConfiguration.Builder): ClientOverrideConfiguration.Builder = {
    clientExecutionTimeout.foreach(overrides.apiCallTimeout)
    requestTimeout.foreach(overrides.apiCallAttemptTimeout)
    userAgent.foreach(overrides.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, _))
    overrides.retryStrategy(maxErrorRetry match {
      case Some(n) => AwsRetryStrategy.standardRetryStrategy().toBuilder.maxAttempts(n + 1).build()
      case None    => AwsRetryStrategy.doNotRetry()
    })
  }

  override def toString: String = {
    val found =
      maxConnections.map("max-connections:" + _) ++
      connectionTimeout.map("connection-timeout:" + _) ++
      socketTimeout.map("socket-timeout:" + _) ++
      clientExecutionTimeout.map("client-execution-timeout:" + _) ++
      requestTimeout.map("request-timeout:" + _) ++
      connectionTtl.map("connection-ttl:" + _) ++
      connectionMaxIdle.map("connection-max-idle-millis:" + _) ++
      useReaper.map("use-reaper:" + _) ++
      useTcpKeepAlive.map("use-tcp-keepalive:" + _) ++
      maxErrorRetry.map("max-error-retry:" + _) ++
      proxyHost.map("proxy-host:" + _) ++
      proxyPort.map("proxy-port:" + _) ++
      proxyUsername.map("proxy-username:" + _) ++
      proxyPassword.map(_ => "proxy-password:<redacted>") ++
      userAgent.map("user-agent:" + _)
    found.mkString("{", ",", "}")
  }
}

object DynamoDBClientConfig {

  /**
   * `aws-client-config` keys from version 1.x that have no AWS SDK v2 equivalent.
   */
  val UnsupportedKeys: List[String] = List(
    "local-address",
    "preemptive-basic-proxy-auth",
    "protocol",
    "proxy-domain",
    "proxy-workstation",
    "response-metadata-cache-size",
    "signer-override",
    "socket-buffer-size-hints",
    "use-expect-continue",
    "use-gzip")
}
