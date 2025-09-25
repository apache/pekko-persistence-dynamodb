/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb.snapshot

import com.amazonaws.services.dynamodbv2.model._
import org.apache.pekko
import pekko.persistence.dynamodb.dynamoClient
import pekko.persistence.dynamodb.journal.DynamoDBHelper
import pekko.actor.ActorSystem
import pekko.util.Timeout

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.jdk.CollectionConverters._

trait DynamoDBUtils {

  def system: ActorSystem
  implicit val executionContext: ExecutionContext = system.dispatcher

  val settings: DynamoDBSnapshotConfig = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("pekko.persistence.snapshot-store.plugin"))
    new DynamoDBSnapshotConfig(config)
  }
  import settings._

  lazy val client: DynamoDBHelper = dynamoClient(system, settings)

  implicit val timeout: Timeout = Timeout(5.seconds)

  import com.amazonaws.services.dynamodbv2.model.{ KeySchemaElement, KeyType }

  val schema = new CreateTableRequest()
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(SequenceNr).withAttributeType("N"),
      new AttributeDefinition().withAttributeName(Timestamp).withAttributeType("N"))
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(SequenceNr).withKeyType(KeyType.RANGE))
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
        .withIndexName(TimestampIndex)
        .withKeySchema(
          new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
          new KeySchemaElement().withAttributeName(Timestamp).withKeyType(KeyType.RANGE))
        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))

  def ensureSnapshotTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema.withTableName(Table).withProvisionedThroughput(new ProvisionedThroughput(read, write))

    var names = Vector.empty[String]
    lazy val complete: ListTablesResult => Future[Vector[String]] = aws =>
      if (aws.getLastEvaluatedTableName == null) Future.successful(names ++ aws.getTableNames.asScala)
      else {
        names ++= aws.getTableNames.asScala
        client
          .listTables(new ListTablesRequest().withExclusiveStartTableName(aws.getLastEvaluatedTableName))
          .flatMap(complete)
      }
    val list = client.listTables(new ListTablesRequest).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains Table)
      _ <- {
        if (exists) Future.successful(())
        else client.createTable(create)
      }
    } yield exists
    val r = Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
}
