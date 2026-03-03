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

package org.apache.pekko.persistence.dynamodb.journal

import software.amazon.awssdk.services.dynamodb.model._
import org.apache.pekko
import pekko.actor.ActorSystem
import pekko.persistence.PersistentRepr
import pekko.persistence.dynamodb._
import pekko.util.Timeout

import java.util.UUID
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait DynamoDBUtils extends JournalSettingsProvider with DynamoProvider {

  def system: ActorSystem

  override val journalSettings = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("pekko.persistence.journal.plugin"))
    new DynamoDBJournalConfig(config)
  }

  override lazy val dynamo: DynamoDBHelper = dynamoClient(system, journalSettings)

  implicit val timeout: Timeout = Timeout(5.seconds)

  def ensureJournalTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create =
      schema
        .tableName(journalSettings.JournalTable)
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(read).writeCapacityUnits(write).build())
        .build()
    implicit val dispatcher = system.dispatcher

    var names = Vector.empty[String]
    lazy val complete: ListTablesResponse => Future[Vector[String]] = aws =>
      if (aws.lastEvaluatedTableName == null) Future.successful(names ++ aws.tableNames.asScala)
      else {
        names ++= aws.tableNames.asScala
        dynamo
          .listTables(ListTablesRequest.builder().exclusiveStartTableName(aws.lastEvaluatedTableName).build())
          .flatMap(complete)
      }
    val list = dynamo.listTables(ListTablesRequest.builder().build()).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains journalSettings.JournalTable)
      _ <- {
        if (exists) Future.successful(())
        else dynamo.createTable(create)
      }
    } yield ()
    Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString
  def persistenceId: String = ???

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

  def persistentRepr(msg: Any) = {
    val ret = PersistentRepr(msg, sequenceNr = seqNr(), persistenceId = persistenceId, writerUuid = writerUuid)
    generatedMessages :+= ret
    ret
  }
}
