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

import com.typesafe.config.Config

trait ClientConfig

trait DynamoDBConfig {
  val AwsKey: String
  val AwsSecret: String
  val Endpoint: String
  val Region: String
  val ClientDispatcher: String
  val client: ClientConfig
  val Tracing: Boolean
  val MaxBatchGet: Int
  val MaxBatchWrite: Int
  val MaxItemSize: Int
  val Table: String
  val JournalName: String

}

class DynamoDBClientConfig(c: Config) extends ClientConfig
