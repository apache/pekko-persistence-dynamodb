/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query.scaladsl

import org.apache.pekko.NotUsed
import org.apache.pekko.persistence.query.EventEnvelope
import org.apache.pekko.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import org.apache.pekko.stream.scaladsl.Source

trait DynamoDBCurrentEventsByPersistenceIdQuery extends CurrentEventsByPersistenceIdQuery {

  /**
   * Same type of query as [[org.apache.pekko.persistence.query.scaladsl.EventsByPersistenceIdQuery.eventsByPersistenceId]]
   * but the event stream is completed immediately when it reaches the end of
   * the results. Events that are stored after the query is completed are
   * not included in the event stream.
   *
   * Execution plan:
   * - a dynamodb <code>query</code> to get lowest sequenceNr
   * - a <code>query</code> per partition. Doing follow calls to get more pages if necessary.
   */
  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long = 0,
      toSequenceNr: Long = Int.MaxValue): Source[EventEnvelope, NotUsed]
}
