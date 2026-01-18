/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query.scaladsl.internal

import org.apache.pekko.NotUsed
import org.apache.pekko.persistence.PersistentRepr
import org.apache.pekko.persistence.dynamodb.journal._
import org.apache.pekko.persistence.dynamodb.query.ReadJournalSettingsProvider
import org.apache.pekko.persistence.dynamodb.query.scaladsl.internal.DynamoDBCurrentEventsByPersistenceIdQuery.RichPersistenceRepr
import org.apache.pekko.persistence.dynamodb.query.scaladsl.{
  DynamoDBCurrentEventsByPersistenceIdQuery => PublicDynamoDBCurrentEventsByPersistenceIdQuery
}
import org.apache.pekko.persistence.dynamodb.{
  ActorSystemProvider, DynamoProvider, LoggingProvider, MaterializerProvider
}
import org.apache.pekko.persistence.query.{ EventEnvelope, Sequence }
import org.apache.pekko.stream.scaladsl.Source

trait DynamoDBCurrentEventsByPersistenceIdQuery
    extends PublicDynamoDBCurrentEventsByPersistenceIdQuery
    with DynamoDBRecovery {
  self: ReadJournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with JournalSettingsProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider =>

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
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(toSequenceNr <= Int.MaxValue, "toSequenceNr can't be bigger than Int.MaxValue")
    require(fromSequenceNr < toSequenceNr, "fromSequenceNr should be smaller than toSequenceNr")
    log.debug("starting currentEventsByPersistenceId for {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)
    Source
      .future(readSequenceNr(persistenceId = persistenceId, highest = true))
      .flatMapConcat { highest =>
        val end = Math.min(highest, toSequenceNr)
        eventsStream(
          persistenceId = persistenceId,
          fromSequenceNr = fromSequenceNr,
          toSequenceNr = end,
          max = Int.MaxValue)
      }
      .map(_.toEventEnvelope)
      .log(s"currentEventsByPersistenceId for $persistenceId from $fromSequenceNr to $toSequenceNr")
  }
}

object DynamoDBCurrentEventsByPersistenceIdQuery {
  implicit class RichPersistenceRepr(val persistenceRepr: PersistentRepr) extends AnyVal {
    def toEventEnvelope =
      new EventEnvelope(
        offset = Sequence(persistenceRepr.sequenceNr),
        persistenceId = persistenceRepr.persistenceId,
        sequenceNr = persistenceRepr.sequenceNr,
        event = persistenceRepr.payload,
        timestamp = persistenceRepr.timestamp)
  }
}
