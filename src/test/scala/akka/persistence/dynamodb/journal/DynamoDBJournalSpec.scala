/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.persistence.journal.JournalSpec
import com.amazonaws.services.dynamodbv2.model.{ CreateTableRequest, DeleteTableRequest, ListTablesRequest, ProvisionedThroughput }
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.persistence.CapabilityFlag
import scala.concurrent.Future

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.ensureJournalTableExists(system)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.on()
}
