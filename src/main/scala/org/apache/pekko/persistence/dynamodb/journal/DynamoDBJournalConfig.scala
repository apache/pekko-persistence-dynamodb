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

import com.typesafe.config.Config

import org.apache.pekko.persistence.dynamodb.{ DynamoDBClientConfig, DynamoDBConfig }

class DynamoDBJournalConfig(c: Config) extends DynamoDBConfig {
  val JournalTable = c.getString("journal-table")
  val Table = JournalTable
  val JournalName = c.getString("journal-name")
  val AwsKey = c.getString("aws-access-key-id")
  val AwsSecret = c.getString("aws-secret-access-key")
  val Endpoint = c.getString("endpoint")
  val ReplayDispatcher = c.getString("replay-dispatcher")
  val ClientDispatcher = c.getString("client-dispatcher")
  val SequenceShards = c.getInt("sequence-shards")
  val ReplayParallelism = c.getInt("replay-parallelism")
  val Tracing = c.getBoolean("tracing")
  val LogConfig = c.getBoolean("log-config")

  val MaxBatchGet = c.getInt("aws-api-limits.max-batch-get")
  val MaxBatchWrite = c.getInt("aws-api-limits.max-batch-write")
  val MaxItemSize = c.getInt("aws-api-limits.max-item-size")

  object Fixes {
    val HighDistrust = c.getBoolean("fixes.high-distrust")
  }

  val client = new DynamoDBClientConfig(c)
  override def toString: String =
    "DynamoDBJournalConfig(" +
    "JournalTable:" + JournalTable +
    ",JournalName:" + JournalName +
    ",AwsKey:" + AwsKey +
    ",Endpoint:" + Endpoint +
    ",ReplayDispatcher:" + ReplayDispatcher +
    ",ClientDispatcher:" + ClientDispatcher +
    ",SequenceShards:" + SequenceShards +
    ",ReplayParallelism" + ReplayParallelism +
    ",Tracing:" + Tracing +
    ",MaxBatchGet:" + MaxBatchGet +
    ",MaxBatchWrite:" + MaxBatchWrite +
    ",MaxItemSize:" + MaxItemSize +
    ",Fixes.HighDistrust:" + Fixes.HighDistrust +
    ",client.config:" + client +
    ")"
}

trait JournalSettingsProvider {
  val journalSettings: DynamoDBJournalConfig
}
