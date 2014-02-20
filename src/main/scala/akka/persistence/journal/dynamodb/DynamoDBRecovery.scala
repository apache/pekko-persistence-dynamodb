package akka.persistence.journal.dynamodb

import akka.persistence.journal.AsyncRecovery
import akka.persistence.PersistentRepr
import scala.concurrent.Future

import DynamoDBJournal._

trait DynamoDBRecovery extends AsyncRecovery{
  this: DynamoDBJournal =>

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString(ReplayDispatcher))


  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logging {
    if (fromSequenceNr > toSequenceNr) return Future.successful(())
    var delivered = 0L
    var maxDeliveredSeq = 0L
    client.sendQuery(queryMessages(processorId, fromSequenceNr)).map {
      qr =>
        import collection.JavaConverters._
        qr.getItems.asScala.foreach {
          item =>
            if (delivered < max && maxDeliveredSeq < toSequenceNr) {
              val repr = readPersistentRepr(item)
              repr.foreach {
                r =>
                  replayCallback(r)
                  delivered += 1
                  maxDeliveredSeq = r.sequenceNr
              }

            }
        }
        qr
    }.flatMap {
      qr =>
        if (qr.getLastEvaluatedKey == null || delivered >= max || maxDeliveredSeq >= toSequenceNr) Future.successful(())
        else {
          val from = qr.getLastEvaluatedKey.get(SequenceNr).getN.toLong
          asyncReplayMessages(processorId, from, toSequenceNr, max - delivered)(replayCallback)
        }
    }
  }



  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = logging {
    client.sendQuery(querySequence(processorId, fromSequenceNr, false)).map {
      qr =>
        if (qr.getItems.size() == 0) {
          log.debug("processorId={} highSequenceNr={}", processorId, 0L)
          0L
        } else {
          val item = qr.getItems.get(0)
          val high = item.get(SequenceNr).getN.toLong
          log.debug("processorId={} fromSequencNr={} highSequenceNr={}", processorId, fromSequenceNr, high)
          high
        }
    }
  }

  def readLowestSequenceNr(processorId: String): Future[Long] = logging {
    client.sendQuery(querySequence(processorId, 1, true)).map {
      qr =>
        if (qr.getItems.size() == 0) {
          log.debug("processorId={} lowSequenceNr={}", processorId, 0L)
          0L
        } else {
          val item = qr.getItems.get(0)
          val low = item.get(SequenceNr).getN.toLong
          log.debug("processorId={} lowSequenceNr={}", processorId, low)
          low
        }
    }
  }

}
