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
import scala.util.{ Try, Success, Failure }
import akka.event.LoggingAdapter
import akka.event.Logging
import scala.util.control.NoStackTrace
import akka.stream.ActorMaterializer

class DynamoDBJournalFailure(message: String) extends RuntimeException(message) with NoStackTrace

class DynamoDBJournal(config: Config) extends AsyncWriteJournal with DynamoDBRecovery with DynamoDBRequests with ActorLogging {
  import context.dispatcher

  implicit val materializer = ActorMaterializer()

  val extension = Persistence(context.system)
  val serialization = SerializationExtension(context.system)

  val settings = new DynamoDBJournalConfig(config)
  if (settings.LogConfig) log.info("using settings {}", settings)

  val dynamo = dynamoClient(context.system, settings)

  dynamo.describeTable(new DescribeTableRequest().withTableName(settings.JournalTable)).onComplete {
    case Success(Right(result)) => log.info("using DynamoDB table {}", result)
    case _ => context match {
      case null =>
      case ctx  => ctx.stop(self)
    }
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    logFailure("write")(Future.sequence(messages.map(writeMessages)))

  // Removed "permanent" as that is no longer used in Akka 2.4
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = logFailure("delete") {
    log.debug("at=delete-messages-to persistenceId={} to={} perm={}", persistenceId, toSequenceNr)
    readSequenceNr(persistenceId, highest = false).flatMap { fromSequenceNr =>
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

  def logFailure[T](desc: String)(f: Future[T]): Future[T] = f.transform(conforms, ex => {
    log.error(ex, "operation failed: " + desc)
    ex
  })
}
