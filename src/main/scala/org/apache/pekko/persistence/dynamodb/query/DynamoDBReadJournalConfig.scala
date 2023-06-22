/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
import org.apache.pekko.persistence.dynamodb.{ ClientConfig, DynamoDBClientConfig, DynamoDBConfig }
import com.typesafe.config.Config

class DynamoDBReadJournalConfig(c: Config) extends DynamoDBConfig {
  val Table: String = c.getString("journal-table")
  val JournalName: String = c.getString("journal-name")
  val AwsKey: String = c.getString("aws-access-key-id")
  val AwsSecret: String = c.getString("aws-secret-access-key")
  val Endpoint: String = c.getString("endpoint")

  val MaxBatchGet: Int = c.getInt("aws-api-limits.max-batch-get")
  val MaxBatchWrite: Int = c.getInt("aws-api-limits.max-batch-write")
  val MaxItemSize: Int = c.getInt("aws-api-limits.max-item-size")

  val PersistenceIdsIndexName: String = c.getString("persistence-ids-index-name")

  override def toString: String =
    "DynamoDBReadJournalConfig(" +
    "Table:" + Table +
    ",AwsKey:" + AwsKey +
    ",Endpoint:" + Endpoint + ")"

  override val client: ClientConfig = new DynamoDBClientConfig(c)

  override val ClientDispatcher: String = c.getString("client-dispatcher")
  override val Tracing: Boolean = c.getBoolean("tracing")

}
object DynamoDBReadJournalConfig {
  def apply()(implicit actorSystem: ActorSystem) =
    new DynamoDBReadJournalConfig(actorSystem.settings.config.getConfig(DynamoDBReadJournal.Identifier))
}
