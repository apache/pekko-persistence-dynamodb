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
