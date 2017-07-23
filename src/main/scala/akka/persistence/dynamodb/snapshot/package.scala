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

}
