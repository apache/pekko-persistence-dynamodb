/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.dynamodb

import com.dimafeng.testcontainers.{ FixedHostPortGenericContainer, ForAllTestContainer }
import org.scalatest.Suite
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

/**
 * Base spec for tests that verify integration with DynamoDB.
 */
trait IntegSpec extends ForAllTestContainer { self: Suite =>
  // TODO: Use dynamic ports. This is a annoying to do as the actor system is init prior to beforeAll.
  override val container: FixedHostPortGenericContainer = FixedHostPortGenericContainer(
    "amazon/dynamodb-local:1.22.0",
    exposedContainerPort = 8000,
    exposedHostPort = 8888,
    waitStrategy = new HttpWaitStrategy().forPath("/").forStatusCode(400))
}
