/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.typesafe.config.Config

class DynamoDBJournalConfig(c: Config) {
  val JournalTable = c getString "journal-table"
  val JournalName = c getString "journal-name"
  val AwsKey = c getString "aws-access-key-id"
  val AwsSecret = c getString "aws-secret-access-key"
  val OpTimeout = c getDuration "operation-timeout"
  val Endpoint = c getString "endpoint"
  val ReplayDispatcher = c getString "replay-dispatcher"
  val ClientDispatcher = c getString "client-dispatcher"
  val SequenceShards = c getInt "sequence-shards"
  val Tracing = c getBoolean "tracing"
}
