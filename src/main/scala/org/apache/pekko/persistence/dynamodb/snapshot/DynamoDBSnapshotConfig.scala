/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb.snapshot

import org.apache.pekko.persistence.dynamodb.{ ClientConfig, DynamoDBClientConfig, DynamoDBConfig }
import com.typesafe.config.Config

class DynamoDBSnapshotConfig(c: Config) extends DynamoDBConfig {
  val Table = c.getString("snapshot-table")
  val JournalName = c.getString("journal-name")
  val AwsKey = c.getString("aws-access-key-id")
  val AwsSecret = c.getString("aws-secret-access-key")
  val Endpoint = c.getString("endpoint")

  val MaxBatchGet = c.getInt("aws-api-limits.max-batch-get")
  val MaxBatchWrite = c.getInt("aws-api-limits.max-batch-write")
  val MaxItemSize = c.getInt("aws-api-limits.max-item-size")

  override def toString: String =
    "DynamoDBJournalConfig(" +
    "SnapshotTable:" + Table +
    ",AwsKey:" + AwsKey +
    ",Endpoint:" + Endpoint + ")"

  override val client: ClientConfig = new DynamoDBClientConfig(c)

  override val ClientDispatcher = c.getString("client-dispatcher")
  override val Tracing = c.getBoolean("tracing")

}
