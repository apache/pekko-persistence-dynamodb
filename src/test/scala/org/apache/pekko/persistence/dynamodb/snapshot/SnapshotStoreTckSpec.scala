/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package org.apache.pekko.persistence.dynamodb.snapshot

import org.apache.pekko.persistence.dynamodb.IntegSpec
import org.apache.pekko.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

class SnapshotStoreTckSpec extends SnapshotStoreSpec(ConfigFactory.load()) with DynamoDBUtils with IntegSpec {
  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureSnapshotTableExists()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    super.afterAll()
  }
}
