package akka.persistence.dynamodb

import java.nio.ByteBuffer
import java.util.{ Map => JMap }

import com.amazonaws.services.dynamodbv2.model._

import scala.collection.generic.CanBuildFrom
import scala.concurrent._
import scala.util.{ Failure, Success, Try }

package object snapshot {

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  // field names
  val Key = "par"
  //  val Sort = "num"
  val Timestamp = "ts"
  val TimestampIndex = "ts-idx"

  val Payload = "pay"
  val SequenceNr = "seq"
  //    val AtomIndex = "idx"
  //    val AtomEnd = "cnt"
  //
  //    val KeyPayloadOverhead = 26 // including fixed parts of partition key and 36 bytes fudge factor

  import com.amazonaws.services.dynamodbv2.model.{ KeySchemaElement, KeyType }

  val schema = new CreateTableRequest()
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(SequenceNr).withAttributeType("N"),
      new AttributeDefinition().withAttributeName(Timestamp).withAttributeType("N")
    )
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(SequenceNr).withKeyType(KeyType.RANGE)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
        .withIndexName(TimestampIndex).withKeySchema(
          new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
          //        new KeySchemaElement().withAttributeName(SequenceNr).withKeyType(KeyType.RANGE),
          new KeySchemaElement().withAttributeName(Timestamp).withKeyType(KeyType.RANGE)
        ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
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
