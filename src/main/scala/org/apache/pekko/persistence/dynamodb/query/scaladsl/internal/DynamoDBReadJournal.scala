/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query.scaladsl.internal

import org.apache.pekko
import pekko.actor.ExtendedActorSystem
import pekko.persistence.dynamodb._
import pekko.persistence.dynamodb.journal._
import pekko.persistence.dynamodb.journal.{ DynamoDBHelper, JournalSettingsProvider }
import pekko.persistence.dynamodb.query.scaladsl.internal.{
  DynamoDBCurrentEventsByPersistenceIdQuery => InternalDynamoDBCurrentEventsByPersistenceIdQuery,
  DynamoDBCurrentPersistenceIdsQuery => InternalDynamoDBCurrentPersistenceIdsQuery
}
import pekko.persistence.dynamodb.query.scaladsl.{ DynamoDBReadJournal => PublicDynamoDBReadJournal }
import pekko.persistence.dynamodb.query.{ DynamoDBReadJournalConfig, ReadJournalSettingsProvider }
import pekko.persistence.query.scaladsl.ReadJournal
import pekko.serialization.{ Serialization, SerializationExtension }
import pekko.stream.{ Materializer, SystemMaterializer }

import com.typesafe.config.Config

class DynamoDBReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with PublicDynamoDBReadJournal
    with InternalDynamoDBCurrentEventsByPersistenceIdQuery
    with InternalDynamoDBCurrentPersistenceIdsQuery
    with ReadJournalSettingsProvider
    with JournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider
    with ActorSystemLoggingProvider {

  protected val readJournalSettings = new DynamoDBReadJournalConfig(config)
  protected val dynamo: DynamoDBHelper = dynamoClient(system, readJournalSettings)
  val serialization: Serialization = SerializationExtension(system)
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  val journalSettings = new DynamoDBJournalConfig(config)

  def close(): Unit = dynamo.shutdown()
}
