/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import java.util.{ HashMap => JHMap, Map => JMap }
import scala.concurrent.ExecutionContext
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import akka.actor.Scheduler
import scala.concurrent.Future

package object journal {

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]
  type AWSTry[+T] = Either[AmazonServiceException, T]

  // field names
  val Key = "key"
  val SequenceNr = "sequenceNr"
  val Payload = "payload"

  import collection.JavaConverters._

  val schema = Seq(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(Key)).asJava
  val schemaAttributes = Seq(new AttributeDefinition().withAttributeName(Key).withAttributeType("S")).asJava

  def dynamoClient(system: ActorSystem, settings: DynamoDBJournalConfig): DynamoDBHelper = {
    val creds = new BasicAWSCredentials(settings.AwsKey, settings.AwsSecret)
    val client = new AmazonDynamoDBAsyncClient(creds)
    client.setEndpoint(settings.Endpoint)
    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(override val ec: ExecutionContext,
                         override val dynamoDB: AmazonDynamoDBAsyncClient,
                         override val settings: DynamoDBJournalConfig,
                         override val scheduler: Scheduler,
                         override val log: LoggingAdapter) extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, system.scheduler, Logging(system, "DynamoDBClient"))
  }
}
