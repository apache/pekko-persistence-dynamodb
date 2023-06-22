/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb

import org.apache.pekko.actor.ExtendedActorSystem
import org.apache.pekko.event.LoggingAdapter
import org.apache.pekko.persistence.dynamodb.journal.{ DynamoDBHelper, DynamoDBJournalConfig }
import org.apache.pekko.stream.Materializer

trait LoggingProvider {
  protected def log: LoggingAdapter
}

trait ActorSystemLoggingProvider extends ActorSystemProvider with LoggingProvider {
  protected val log: LoggingAdapter = system.log
}

trait ActorSystemProvider {
  protected implicit val system: ExtendedActorSystem
}

trait DynamoProvider {
  protected def dynamo: DynamoDBHelper
}

trait MaterializerProvider {
  protected implicit val materializer: Materializer
}
