package akka.persistence.dynamodb

import akka.actor.ExtendedActorSystem
import akka.event.LoggingAdapter
import akka.persistence.dynamodb.journal.{ DynamoDBHelper, DynamoDBJournalConfig }
import akka.stream.Materializer

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
