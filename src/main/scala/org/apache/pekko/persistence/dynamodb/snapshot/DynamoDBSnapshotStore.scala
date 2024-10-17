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

import org.apache.pekko.actor.ActorLogging
import org.apache.pekko.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import org.apache.pekko.persistence.dynamodb._
import org.apache.pekko.persistence.snapshot.SnapshotStore
import org.apache.pekko.serialization.SerializationExtension
import com.typesafe.config.Config

import scala.concurrent.Future

class DynamoDBSnapshotRejection(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

class DynamoDBSnapshotStore(config: Config) extends SnapshotStore with DynamoDBSnapshotRequests with ActorLogging {
  val journalSettings = new DynamoDBSnapshotConfig(config)
  val dynamo = dynamoClient(context.system, journalSettings)
  val serialization = SerializationExtension(context.system)
  import journalSettings._

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! DynamoDBSnapshotStore.Init
  }

  override def postStop(): Unit =
    dynamo.shutdown()

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    load(persistenceId, criteria)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    save(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, snapshot)

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    delete(metadata)

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    delete(persistenceId, criteria)

}

object DynamoDBSnapshotStore {
  private case object Init
}
