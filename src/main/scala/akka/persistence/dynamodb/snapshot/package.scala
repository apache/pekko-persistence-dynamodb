/**
  * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
  */
package akka.persistence.dynamodb

import java.nio.ByteBuffer

import com.amazonaws.services.dynamodbv2.model._

import scala.collection.generic.CanBuildFrom
import scala.concurrent._
import scala.util.{ Failure, Success, Try }

package object snapshot {

  // field names
  val Key = "par"
  val Timestamp = "ts"
  val TimestampIndex = "ts-idx"

  val Payload = "pay"
  val SequenceNr = "seq"

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
          new KeySchemaElement().withAttributeName(Timestamp).withKeyType(KeyType.RANGE)
        ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
    )
}
