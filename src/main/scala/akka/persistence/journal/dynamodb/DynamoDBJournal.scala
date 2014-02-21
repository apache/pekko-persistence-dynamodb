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
  val dynamo = dynamoClient(context.system, context, config)
  val journalName = config.getString(JournalName)
  val sequenceShards = 1000
  val maxDynamoBatchGet = 100

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  def asyncWriteMessages(messages: immutable.Seq[PersistentRepr]): Future[Unit] = writeMessages(messages)

  //do we need to store the confirmations in a separate key to avoid hot keys?
  def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = writeConfirmations(confirmations)

  def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = deleteMessages(messageIds, permanent)

  def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] =  {
    log.debug("at=delete-messages-to processorId={} to={} perm={}", processorId, toSequenceNr, permanent)
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

  def SS(value: String): AttributeValue = new AttributeValue().withSS(value)

  def SS(values: Seq[String]): AttributeValue = new AttributeValue().withSS(values: _*)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def US(value: String): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(SS(value))

  def messageKey(procesorId: String, sequenceNr: Long) = S(str("P-", procesorId, "-", sequenceNr))

  def highSeqKey(procesorId: String, sequenceNr: Long) = S(str("SH-", procesorId, "-", sequenceNr))

  def lowSeqKey(procesorId: String, sequenceNr: Long) = S(str("SL-", procesorId, "-", sequenceNr))

  def str(ss: Any*): String = ss.foldLeft(new StringBuilder)(_.append(_)).toString()

  def persistentToByteBuffer(p: PersistentRepr): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr = {
    serialization.deserialize(ByteString(b).toArray, classOf[PersistentRepr]).get
  }

  def logging[T](f: Future[T]): Future[T] = {
    f.onFailure {
      case e: Exception =>
        log.error(e, "error in async op")
        e.printStackTrace
    }
    f
  }

}

class InstrumentedDynamoDBClient(props: DynamoDBClientProps) extends DynamoDBClient(props) {
  def logging[T](op: String)(f: Future[T]): Future[T] = {
    f.onFailure {
      case e: Exception => props.system.log.error(e, "error in async op {}", op)
    }
    f
  }

  override def sendBatchWriteItem(awsWrite: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    logging("sendBatchWriteItem")(super.sendBatchWriteItem(awsWrite))

  override def sendBatchGetItem(awsWrite: BatchGetItemRequest): Future[BatchGetItemResult] =
    logging("sendBatchWriteItem")(super.sendBatchGetItem( awsWrite))

  override def sendUpdateItem(aws: UpdateItemRequest): Future[UpdateItemResult] =
    logging("sendBatchWriteItem")(super.sendUpdateItem(aws))

  override def sendPutItem(aws: PutItemRequest): Future[PutItemResult] =
    logging("sendBatchWriteItem")(super.sendPutItem(aws))

}

object DynamoDBJournal {
  // field names
  val Key = "key"
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

  val schema = Seq(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(Key)).asJava
  val schemaAttributes = Seq(new AttributeDefinition().withAttributeName(Key).withAttributeType("S")).asJava

  def dynamoClient(system: ActorSystem, context: ActorRefFactory, config: Config): DynamoDBClient = {
    val props = DynamoDBClientProps(
      config.getString(AwsKey),
      config.getString(AwsSecret),
      config.getDuration(OpTimeout, TimeUnit.MILLISECONDS) milliseconds,
      system,
      context,
      config.getString(Endpoint)
    )
    new InstrumentedDynamoDBClient(props)
  }


}