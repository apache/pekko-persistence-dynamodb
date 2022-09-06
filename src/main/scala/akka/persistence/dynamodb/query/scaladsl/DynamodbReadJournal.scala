package akka.persistence.dynamodb.query.scaladsl

/**
 * Scala API `akka.persistence.query.scaladsl.ReadJournal` implementation for Dynamodb.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[DynamodbReadJournal](DynamodbReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.dynamodb.query.javadsl.DynamodbReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamodbReadJournal#Identifier]]. See `reference.conf`.
 */
trait DynamodbReadJournal extends DynamodbCurrentEventsByPersistenceIdQuery with DynamodbCurrentPersistenceIdsQuery

object DynamodbReadJournal {

  /**
   * The default identifier for [[DynamodbReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"dynamodb-read-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  val Identifier = "dynamodb-read-journal"
}
