/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.nio.ByteBuffer
import java.util.{ HashMap => JHMap, Map => JMap }
import akka.actor.{ ActorLogging, ActorRefFactory, ActorSystem }
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ AtomicWrite, Persistence, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.util.ByteString
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.Config
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import akka.event.LoggingAdapter
import akka.event.Logging

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with DynamoDBRecovery with DynamoDBRequests with ActorLogging {
  import DynamoDBJournal._
  import context.dispatcher

  val extension = Persistence(context.system)
  val serialization = SerializationExtension(context.system)
  val dynamo = dynamoClient(context.system, config)

  val settings = new DynamoDBJournalConfig(config)

  val journalTable = settings.JournalTable
  val sequenceShards = settings.SequenceShards

  val maxDynamoBatchGet = 100
  val replayParallelism = 10

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    Future.sequence(messages.map(writeMessages))

  // Removed "permanent" as that is no longer used in Akka 2.4
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("at=delete-messages-to persistenceId={} to={} perm={}", persistenceId, toSequenceNr)
    readLowestSequenceNr(persistenceId).flatMap { fromSequenceNr =>
      asyncReadHighestSequenceNr(persistenceId, fromSequenceNr).flatMap { highestSequenceNr =>
        val end = Math.min(toSequenceNr, highestSequenceNr)
        val asyncDeletions = (fromSequenceNr to end).grouped(12).map(deleteMessages(persistenceId, _))
        Future.sequence(asyncDeletions).map(_ => log.debug("finished asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr))
      }
    }
  }

  // Maps a sequence of tuples to a hashmap
  def fields[T](fs: (String, T)*): JMap[String, T] = {
    val map = new JHMap[String, T]()
    fs.foreach {
      case (k, v) => map.put(k, v)
    }
    map
  }

  def withBackoff[I, O](i: I, retriesRemaining: Int = 10)(op: I => Future[Either[AmazonServiceException, O]]): Future[O] = {
    op(i).flatMap {
      case Left(t: ProvisionedThroughputExceededException) =>
        backoff(10 - retriesRemaining, i.getClass.getSimpleName)
        withBackoff(i, retriesRemaining - 1)(op)
      case Left(e) =>
        log.error(e, "exception in withBackoff")
        throw e
      case Right(resp) =>
        Future.successful(resp)
    }
  }

  def backoff(retries: Int, what: String) {
    if (retries == 0) Thread.`yield`()
    else {
      val sleep = math.pow(2, retries).toLong
      log.warning("at=backoff request={} sleep={}", what, sleep)
      Thread.sleep(sleep)
    }
  }

  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def S(value: Boolean): AttributeValue = new AttributeValue().withS(value.toString)

  def N(value: Long): AttributeValue = new AttributeValue().withN(value.toString)

  def SS(value: String): AttributeValue = new AttributeValue().withSS(value)

  def SS(values: Seq[String]): AttributeValue = new AttributeValue().withSS(values: _*)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def US(value: String): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(SS(value))

  private val journalName = settings.JournalName

  def messageKey(persistenceId: String, sequenceNr: Long) = S(s"$journalName-P-$persistenceId-$sequenceNr")

  def highSeqKey(persistenceId: String, sequenceNr: Long) = S(s"$journalName-SH-$persistenceId-$sequenceNr")

  def lowSeqKey(persistenceId: String, sequenceNr: Long) = S(s"$journalName-SL-$persistenceId-$sequenceNr")

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(ByteString(b).toArray, classOf[PersistentRepr]).get
  }

}

object DynamoDBJournal {
  // field names
  val Key = "key"
  val PersistenceId = "persistenceId"
  val SequenceNr = "sequenceNr"
  val Deleted = "deleted"
  val Payload = "payload"
  // config names

  import collection.JavaConverters._

  val schema = Seq(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(Key)).asJava
  val schemaAttributes = Seq(new AttributeDefinition().withAttributeName(Key).withAttributeType("S")).asJava

  def dynamoClient(system: ActorSystem, config: Config): DynamoDBHelper = {
    val settings = new DynamoDBJournalConfig(config)
    val creds = new BasicAWSCredentials(settings.AwsKey, settings.AwsSecret)
    val client = new AmazonDynamoDBClient(creds)
    client.setEndpoint(settings.Endpoint)
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    new DynamoDBClient(dispatcher, client, settings.Tracing, Logging(system, "DynamoDBClient"))
  }

  private class DynamoDBClient(override val ec: ExecutionContext,
                               override val dynamoDB: AmazonDynamoDBClient,
                               override val tracing: Boolean,
                               override val log: LoggingAdapter) extends DynamoDBHelper
}
