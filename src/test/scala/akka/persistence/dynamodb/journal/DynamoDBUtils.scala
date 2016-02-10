/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.Await
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.persistence.Persistence
import akka.util.Timeout
import java.util.UUID
import akka.persistence.PersistentRepr

trait DynamoDBUtils {

  val system: ActorSystem
  import system.dispatcher

  lazy val settings = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.journal.plugin"))
    new DynamoDBJournalConfig(config)
  }
  import settings._

  lazy val client = dynamoClient(system, settings)

  implicit val timeout = Timeout(5.seconds)

  def ensureJournalTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val describe = new DescribeTableRequest().withTableName(JournalTable)
    val create = schema
      .withTableName(JournalTable)
      .withProvisionedThroughput(new ProvisionedThroughput(read, write))

    val setup = for {
      exists <- client.describeTable(describe).map(_ => true).recover { case _ => false }
      _ <- {
        if (exists) Future.successful(())
        else client.createTable(create)
      }
    } yield ()
    Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString
  def persistenceId: String = ???

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

  def persistentRepr(msg: Any) = {
    val ret = PersistentRepr(msg, sequenceNr = seqNr(), persistenceId = persistenceId, writerUuid = writerUuid)
    generatedMessages :+= ret
    ret
  }
}
