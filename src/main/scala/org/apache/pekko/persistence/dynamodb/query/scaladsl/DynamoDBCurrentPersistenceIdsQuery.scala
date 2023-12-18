/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb.query.scaladsl

import org.apache.pekko.NotUsed
import org.apache.pekko.persistence.dynamodb.DynamoProvider
import org.apache.pekko.persistence.dynamodb.query.ReadJournalSettingsProvider
import org.apache.pekko.persistence.dynamodb.query.scaladsl.CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest
import org.apache.pekko.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import org.apache.pekko.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.Future

trait DynamoDBCurrentPersistenceIdsQuery extends CurrentPersistenceIdsQuery {

  /**
   * Same type of query as [[org.apache.pekko.persistence.query.scaladsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  override def currentPersistenceIds(): Source[String, NotUsed]

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPageScan(): Source[Seq[String], NotUsed]

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  def currentPersistenceIdsByPageQuery(): Source[Seq[String], NotUsed]

  /**
   * Persistence ids are returned alphabetically page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsAlphabeticallyIndexRequest]]
   */
  def currentPersistenceIdsAlphabeticallyByPageQuery(
      fromPersistenceId: Option[String] = None): Source[Seq[String], NotUsed]
}
trait CreatePersistenceIdsIndex {
  self: ReadJournalSettingsProvider with DynamoProvider =>

  /**
   * Update the journal table to add the Global Secondary Index 'persistence-ids-idx' that's required by [[DynamoDBCurrentPersistenceIdsQuery.currentPersistenceIdsByPageQuery]]
   * @param alphabetically sort persistence ids
   */
  def createPersistenceIdsIndex(alphabetically: Boolean = false): Future[UpdateTableResult] =
    dynamo.updateTable(
      createPersistenceIdsIndexRequest(
        indexName = readJournalSettings.PersistenceIdsIndexName,
        tableName = readJournalSettings.Table,
        alphabetically = alphabetically))
}

object CreatePersistenceIdsIndex {

  /**
   * required by [[DynamoDBCurrentPersistenceIdsQuery.currentPersistenceIdsByPageQuery]]
   *
   * When requesting snapshots by timestamp, Select ALL_ATTRIBUTES is used. Thus, a duplicate of
   * the payload doesn't need to be stored in the index, which is more space efficient.
   * This allows to make the choice between time and space efficiency by selecting the projection
   * strategy for the created index.
   */
  def createPersistenceIdsIndexRequest(
      indexName: String,
      tableName: String,
      alphabetically: Boolean = false): UpdateTableRequest = {
    val createIndex = new CreateGlobalSecondaryIndexAction()
      .withIndexName(indexName)
      .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
      .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10).withWriteCapacityUnits(10))
    if (alphabetically) {
      createIndex.withKeySchema(
        new KeySchemaElement().withAttributeName("num").withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName("par").withKeyType(KeyType.RANGE))
    } else {
      createIndex.withKeySchema(new KeySchemaElement().withAttributeName("num").withKeyType(KeyType.HASH))
    }
    new UpdateTableRequest()
      .withTableName(tableName)
      .withGlobalSecondaryIndexUpdates(new GlobalSecondaryIndexUpdate().withCreate(createIndex))
      .withAttributeDefinitions(new AttributeDefinition().withAttributeName("num").withAttributeType("N"))
  }

  /** required by [[DynamoDBCurrentPersistenceIdsQuery.currentPersistenceIdsAlphabeticallyByPageQuery]] */
  def createPersistenceIdsAlphabeticallyIndexRequest(indexName: String, tableName: String): UpdateTableRequest =
    createPersistenceIdsIndexRequest(indexName = indexName, tableName = tableName, alphabetically = true)
}
