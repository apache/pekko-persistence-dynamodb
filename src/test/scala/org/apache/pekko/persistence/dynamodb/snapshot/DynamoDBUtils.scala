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

import software.amazon.awssdk.services.dynamodb.model._
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

  val schema = CreateTableRequest.builder()
    .attributeDefinitions(
      AttributeDefinition.builder().attributeName(Key).attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName(SequenceNr).attributeType(ScalarAttributeType.N).build(),
      AttributeDefinition.builder().attributeName(Timestamp).attributeType(ScalarAttributeType.N).build())
    .keySchema(
      KeySchemaElement.builder().attributeName(Key).keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName(SequenceNr).keyType(KeyType.RANGE).build())
    .localSecondaryIndexes(
      LocalSecondaryIndex.builder()
        .indexName(TimestampIndex)
        .keySchema(
          KeySchemaElement.builder().attributeName(Key).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(Timestamp).keyType(KeyType.RANGE).build())
        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
        .build())

  def ensureSnapshotTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema.tableName(Table)
      .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(read).writeCapacityUnits(write).build())
      .build()

    var names = Vector.empty[String]
    lazy val complete: ListTablesResponse => Future[Vector[String]] = aws =>
      if (aws.lastEvaluatedTableName == null) Future.successful(names ++ aws.tableNames.asScala)
      else {
        names ++= aws.tableNames.asScala
        client
          .listTables(ListTablesRequest.builder().exclusiveStartTableName(aws.lastEvaluatedTableName).build())
          .flatMap(complete)
      }
    val list = client.listTables(ListTablesRequest.builder().build()).flatMap(complete)

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
