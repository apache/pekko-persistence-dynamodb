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

package org.apache.pekko.persistence.dynamodb.journal

import java.util.{ HashMap => JHMap, Map => JMap }
import org.apache.pekko.Done
import org.apache.pekko.actor.{ ActorLogging, ActorRef, ExtendedActorSystem }
import org.apache.pekko.dispatch.ExecutionContexts
import org.apache.pekko.pattern.pipe
import org.apache.pekko.persistence.journal.AsyncWriteJournal
import org.apache.pekko.persistence.{ AtomicWrite, Persistence }
import org.apache.pekko.persistence.dynamodb._
import org.apache.pekko.serialization.{ Serialization, SerializationExtension }
import org.apache.pekko.stream.{ Materializer, SystemMaterializer }
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ Future, Promise }
import scala.util.{ Success, Try }

class DynamoDBJournalFailure(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
class DynamoDBJournalRejection(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
 * Query the table for all sequence numbers of the given persistenceId, starting
 * from zero upwards and finishing when encountering a run of at least MaxBatchGet
 * missing entries. This is a potentially very expensive operation, use with care!
 *
 * A response of type [[ListAllResult]] will be sent back to the given `replyTo`
 * reference.
 */
case class ListAll(persistenceId: String, replyTo: ActorRef)

/**
 * Response to the [[ListAll]] request, containing
 *
 *  - the persistenceId
 *  - the set of lowest sequence numbers stored in the sequence shards
 *  - the set of highest sequence numbers stored in the sequence shards
 *  - the sequence numbers of all stored events in ascending order
 *
 * The lowest/highest sequence number is obtained by taking the maximum of either set.
 */
case class ListAllResult(persistenceId: String, lowest: Set[Long], highest: Set[Long], events: Seq[Long])

/**
 * Purge all information stored for the given `persistenceId` from the journal.
 * Purging the information for a running actor results in undefined behavior.
 *
 * A confirmation of type [[Purged]] will be sent to the given `replyTo` reference.
 */
case class Purge(persistenceId: String, replyTo: ActorRef)

private[pekko] case class SetDBHelperReporter(ref: ActorRef)

/**
 * Confirmation that all information stored for the given `persistenceId` has
 * been purged from the journal.
 */
case class Purged(persistenceId: String)

class DynamoDBJournal(config: Config)
    extends AsyncWriteJournal
    with DynamoDBRecovery
    with DynamoDBJournalRequests
    with ActorLogging
    with DynamoProvider
    with JournalSettingsProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider {
  import context.dispatcher

  protected implicit val system: ExtendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]
  implicit val materializer: Materializer = SystemMaterializer(context.system).materializer

  val extension = Persistence(context.system)
  val serialization: Serialization = SerializationExtension(context.system)

  val journalSettings = new DynamoDBJournalConfig(config)

  import journalSettings._
  if (LogConfig) log.info("using settings {}", journalSettings)

  val dynamo = dynamoClient(context.system, journalSettings)

  dynamo.describeTable(new DescribeTableRequest().withTableName(JournalTable)).onComplete {
    case Success(result) => log.info("using DynamoDB table {}", result)
    case _               => log.error("persistent actor requests will fail until the table '{}' is accessible", JournalTable)
  }

  override def postStop(): Unit = dynamo.shutdown()

  private case class OpFinished(pid: String, f: Future[Done])
  private val opQueue: JMap[String, Future[Done]] = new JHMap

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val p = Promise[Done]
    val pid = messages.head.persistenceId
    opQueue.put(pid, p.future)

    val f = writeMessages(messages)

    f.onComplete { _ =>
      self ! OpFinished(pid, p.future)
      p.success(Done)
    }(ExecutionContexts.sameThreadExecutionContext)

    f
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    opQueue.get(persistenceId) match {
      case null =>
        logFailure(s"read-highest-sequence-number($persistenceId)")(readSequenceNr(persistenceId, highest = true))
      case f =>
        f.flatMap(_ => logFailure(s"read-highest($persistenceId)")(readSequenceNr(persistenceId, highest = true)))
    }

  /**
   * Delete messages up to a given sequence number. The range to which this applies
   * first capped by the lowest and highest sequence number for this persistenceId
   * since DynamoDB requires individual deletes to be issued for every single event.
   * The procedure is to first update the lowest sequence number to the new value
   * and then delete the now unreplayable events—this is desirable because in the
   * other order a replay may see partially deleted history.
   *
   * Failures during purging are only logged and do not contribute to the call’s
   * result.
   *
   * TODO in principle replays should be inhibited while the purge is ongoing
   */
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    logFailure(s"delete($persistenceId, upto=$toSequenceNr)") {
      log.debug("delete-messages persistenceId={} to={}", persistenceId, toSequenceNr)
      val lowF = readSequenceNr(persistenceId, highest = false)
      val highF = readSequenceNr(persistenceId, highest = true)
      for {
        lowest <- lowF
        highest <- highF
        upTo = Math.min(toSequenceNr, highest)
        _ <- if (upTo + 1 > lowest) setLS(persistenceId, to = upTo + 1) else Future.successful(Done)
        _ <- if (lowest <= upTo) deleteMessages(persistenceId, lowest, upTo) else Future.successful(Done)
      } yield {
        log.debug("finished asyncDeleteMessagesTo {} {} ({})", persistenceId, toSequenceNr, upTo)
      }
    }

  private def listAll(persistenceId: String): Future[ListAllResult] =
    for {
      low <- readAllSequenceNr(persistenceId, highest = false)
      high <- readAllSequenceNr(persistenceId, highest = true)
      seqs <- listAllSeqNr(persistenceId)
    } yield ListAllResult(persistenceId, low, high, seqs)

  private def purge(persistenceId: String): Future[Done] =
    for {
      highest <- readSequenceNr(persistenceId, highest = true)
      _ <- deleteMessages(persistenceId, 0, highest)
      _ <- removeLS(persistenceId)
      _ <- removeHS(persistenceId)
    } yield Done

  override def receivePluginInternal = {
    case OpFinished(persistenceId, f)    => opQueue.remove(persistenceId, f)
    case ListAll(persistenceId, replyTo) => listAll(persistenceId).pipeTo(replyTo)
    case Purge(persistenceId, replyTo)   => purge(persistenceId).map(_ => Purged(persistenceId)).pipeTo(replyTo)
    case SetDBHelperReporter(ref)        => dynamo.setReporter(ref)
  }
}

trait JournalKeys { self: JournalSettingsProvider =>
  import journalSettings._
  def keyLength(persistenceId: String, sequenceNr: Long): Int =
    persistenceId.length + JournalName.length + KeyPayloadOverhead

  def messageKey(persistenceId: String, sequenceNr: Long): Item = {
    val item: Item = new JHMap
    item.put(Key, S(messagePartitionKey(persistenceId, sequenceNr)))
    item.put(Sort, N(sequenceNr % PartitionSize))
    item
  }

  def messagePartitionKey(persistenceId: String, sequenceNr: Long): String =
    messagePartitionKeyFromGroupNr(persistenceId, sequenceNr / PartitionSize)

  def messagePartitionKeyFromGroupNr(persistenceId: String, partitionGroupNr: Long): String =
    s"$JournalName-P-$persistenceId-$partitionGroupNr"

  def highSeqKey(persistenceId: String, shard: Long) = {
    val item: Item = new JHMap
    item.put(Key, S(s"$JournalName-SH-$persistenceId-$shard"))
    item.put(Sort, Naught)
    item
  }

  def lowSeqKey(persistenceId: String, shard: Long) = {
    val item: Item = new JHMap
    item.put(Key, S(s"$JournalName-SL-$persistenceId-$shard"))
    item.put(Sort, Naught)
    item
  }

}

trait SerializationProvider {
  def serialization: Serialization
}
