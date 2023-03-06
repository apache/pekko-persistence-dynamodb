/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

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
