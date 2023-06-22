/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query.scaladsl

/**
 * Scala API `org.apache.pekko.persistence.query.scaladsl.ReadJournal` implementation for DynamoDB.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[org.apache.pekko.persistence.dynamodb.query.javadsl.DynamoDBReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamoDBReadJournal#Identifier]]. See `reference.conf`.
 */
trait DynamoDBReadJournal extends DynamoDBCurrentEventsByPersistenceIdQuery with DynamoDBCurrentPersistenceIdsQuery {
  def close(): Unit
}

object DynamoDBReadJournal {

  /**
   * The default identifier for [[DynamoDBReadJournal]] to be used with
   * `org.apache.pekko.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"dynamodb-read-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  val Identifier = "dynamodb-read-journal"
}
