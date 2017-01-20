package akka.persistence.dynamodb

import akka.persistence.dynamodb.journal.{ DynamoDBConfig, DynamoDBHelper }
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.{ Logging, LoggingAdapter }
import java.util.{ Map => JMap }
import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.{ ThreadPoolExecutor, LinkedBlockingQueue, TimeUnit }
import scala.collection.generic.CanBuildFrom
import java.util.concurrent.Executors
import java.util.Collections
import java.nio.ByteBuffer

package object snapshot {

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  // field names
  val Key = "par"
  val Sort = "num"
  //    val Payload = "pay"
  //    val SequenceNr = "seq"
  //    val AtomIndex = "idx"
  //    val AtomEnd = "cnt"
  //
  //    val KeyPayloadOverhead = 26 // including fixed parts of partition key and 36 bytes fudge factor

  import collection.JavaConverters._

  val schema = new CreateTableRequest()
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(Sort).withKeyType(KeyType.RANGE)
    )
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(Sort).withAttributeType("N")
    )

  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def N(value: Long): AttributeValue = new AttributeValue().withN(value.toString)
  def N(value: String): AttributeValue = new AttributeValue().withN(value)
  val Naught = N(0)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def lift[T](f: Future[T]): Future[Try[T]] = {
    val p = Promise[Try[T]]
    f.onComplete(p.success)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def liftUnit(f: Future[Any]): Future[Try[Unit]] = {
    val p = Promise[Try[Unit]]
    f.onComplete {
      case Success(_)     => p.success(Success(()))
      case f @ Failure(_) => p.success(f.asInstanceOf[Failure[Unit]])
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def trySequence[A, M[X] <: TraversableOnce[X]](in: M[Future[A]])(implicit
    cbf: CanBuildFrom[M[Future[A]], Try[A], M[Try[A]]],
                                                                   executor: ExecutionContext): Future[M[Try[A]]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      val fb = lift(a)
      for (r <- fr; b <- fb) yield (r += b)
    }.map(_.result())

}
