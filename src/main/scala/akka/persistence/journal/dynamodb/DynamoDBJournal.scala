package akka.persistence.journal.dynamodb

import DynamoDBJournal._
import akka.actor.{ActorLogging, ActorRefFactory, ActorSystem}
import akka.persistence._
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension
import akka.util.ByteString
import com.amazonaws.services.dynamodbv2.model._
import com.sclasen.spray.aws.dynamodb.DynamoDBClient
import com.sclasen.spray.aws.dynamodb.DynamoDBClientProps
import com.typesafe.config.Config
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.{HashMap => JHMap, Map => JMap}
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

class DynamoDBJournal extends AsyncWriteJournal with DynamoDBRecovery with DynamoDBRequests with ActorLogging {

  val config = context.system.settings.config.getConfig(Conf)
  val extension = Persistence(context.system)
  val serialization = SerializationExtension(context.system)
  val client = dynamoClient(context.system, context, config)
  val journalName = config.getString(JournalName)

  def asyncWriteMessages(messages: immutable.Seq[PersistentRepr]): Future[Unit] = Future.sequence {
    messages.map {
      msg =>
        client.sendPutItem(persistentToPut(msg)).map(_ => (msg.processorId, msg.sequenceNr))
    }
  }.map {
    msgs => log.debug("wroteMessages {}", msgs)
  }

  def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = Future.sequence {
    confirmations.map {
      msg =>
        client.sendUpdateItem(confirmationToUpdate(msg))
    }
  }.map {
    _ => log.debug("wroteConfirmations {}", confirmations)
  }


  def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = logging {
    Future.sequence {
      messageIds.map {
        msg =>
          if (permanent) {
            client.sendDeleteItem(permanentDeleteToDelete(msg)).map(_ => msg)
          } else {
            client.sendUpdateItem(impermanentDeleteToUpdate(msg)).map(_ => msg)
          }
      }
    }.map {
      ids =>
        log.debug("deleted {}", messageIds)
    }
  }

  def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = logging {
    readLowestSequenceNr(processorId).flatMap {
      fromSequenceNr =>
        val asyncDeletions = (fromSequenceNr to toSequenceNr).grouped(extension.settings.journal.maxDeletionBatchSize).map {
          group =>
            asyncDeleteMessages(group.map(sequenceNr => PersistentIdImpl(processorId, sequenceNr)), permanent)
        }
        Future.sequence(asyncDeletions).map(_ => log.debug("finished asyncDeleteMessagesTo {} {} {}", processorId, toSequenceNr, permanent))
    }
  }


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

  def SS(value: String): AttributeValue = new AttributeValue().withSS(value.toString)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def US(value: String): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(SS(value))

  def str(ss: Any*): String = ss.foldLeft(new StringBuilder)(_.append(_)).toString()

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(ByteString(b).toArray, classOf[PersistentRepr]).get
  }

  def logging[T](f: Future[T]): Future[T] = {
    f.onFailure {
      case e: Exception => log.error(e, "error in async op")
    }
    f
  }

}

object DynamoDBJournal {
  // field names
  val ProcessorId = "processorId"
  val SequenceNr = "sequenceNr"
  val Confirmations = "confirmations"
  val Deleted = "deleted"
  val Payload = "payload"
  // config names
  val Conf = "dynamodb-journal"
  val JournalName = "journal-name"
  val AwsKey = "aws-access-key-id"
  val AwsSecret = "aws-secret-access-key"
  val OpTimeout = "operation-timeout"
  val Endpoint = "endpoint"
  val ReplayDispatcher = "replay-dispatcher"

  import collection.JavaConverters._

  val schema = Seq(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(ProcessorId), new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(SequenceNr)).asJava
  val schemaAttributes = Seq(new AttributeDefinition().withAttributeName(ProcessorId).withAttributeType("S"), new AttributeDefinition().withAttributeName(SequenceNr).withAttributeType("N")).asJava

  def dynamoClient(system: ActorSystem, context: ActorRefFactory, config: Config): DynamoDBClient = {
    val props = DynamoDBClientProps(
      config.getString(AwsKey),
      config.getString(AwsSecret),
      config.getDuration(OpTimeout, TimeUnit.MILLISECONDS) milliseconds,
      system,
      context,
      config.getString(Endpoint)
    )
    new DynamoDBClient(props)
  }


}