package akka.persistence.journal.dynamodb

import DynamoDBJournal._
import akka.actor.{PoisonPill, ActorRef, Actor}
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import collection.JavaConverters._
import collection.immutable
import com.amazonaws.services.dynamodbv2.model._
import java.util.Collections
import java.util.{HashMap => JHMap, Map => JMap, List => JList}
import scala.concurrent.Future


trait DynamoDBRecovery extends AsyncRecovery {
  this: DynamoDBJournal =>

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString(ReplayDispatcher))


  /*def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    asyncReadHighestSequenceNr(processorId, fromSequenceNr).flatMap {
      highest =>
        val to = Seq(highest, toSequenceNr).min
        log.debug("at=async-replay-messages processorId={} from={} to={} max={}", processorId, fromSequenceNr, toSequenceNr, max)
        implicit val timeout = Timeout(5 minutes)
        if (fromSequenceNr > to) return Future.successful(())
        val replayer = context.actorOf(Props(classOf[ReplayResequencer], fromSequenceNr, to, max, replayCallback))
        val done = replayer ? ReplayDone
        val reads = Stream.iterate(fromSequenceNr, (to + 1 - fromSequenceNr ).toInt)(_ + 1).map {
          sequenceNr => sequenceNr -> fields(Key -> messageKey(processorId, sequenceNr))
        }.grouped(maxDynamoBatchGet).map {
          keys =>
            val ka = new KeysAndAttributes().withKeys(keys.map(_._2).asJava).withConsistentRead(true)
            val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
            log.debug("replay at=batch-request keys={}", keys.size)
            dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
              result =>
                val batchMap = mapBatch(result.getResponses.get(journalName))
                log.debug("replay at=batch-response responses={}", batchMap.size)
                keys.foreach {
                  case (sequenceNr, key) =>
                    val item = batchMap.get(key.get(Key))
                    log.debug("replay at=send-to-resequencer")
                    replayer ! (sequenceNr, readPersistentRepr(item))
                }
            }
        }
        Future.sequence(Seq(done, Future.sequence(reads.toList))).map(_ => ())
    }
  }*/


  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logging {
    if (fromSequenceNr > toSequenceNr) return Future.successful(())
    var delivered = 0L
    var maxDeliveredSeq = 0L
    val keys = Stream.iterate(fromSequenceNr, 100)(_ + 1).map(s => s -> fields(Key -> messageKey(processorId, s)))
    val ka = new KeysAndAttributes().withKeys(keys.map(_._2).asJava).withConsistentRead(true).withAttributesToGet(Key, Payload, Deleted, Confirmations)
    val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
    log.debug("replay at=batch-request keys={}", keys.size)
    //todo send 10 concurrent batch gets
    dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
      result =>
        val batchMap = mapBatch(result.getResponses.get(journalName))
        log.debug("replay at=batch-response responses={}", batchMap.keySet().toString)
        keys.foreach {
          case (sequenceNr, key) =>
            val k = key.get(Key)
            Option(batchMap.get(k)).map {
              item =>
                val repr = readPersistentRepr(item)
                repr.foreach {
                  r =>
                    if (delivered < max && maxDeliveredSeq < toSequenceNr) {
                      replayCallback(r)
                      delivered += 1
                      maxDeliveredSeq = r.sequenceNr
                      log.debug("deliver {} {}", processorId, sequenceNr)
                      log.debug("delivered {} max {}", delivered, max)
                      log.debug("ms {} to {}", maxDeliveredSeq, toSequenceNr)
                    }
                }
            }
        }
        log.debug("delivered batch")
        batchMap.size()
    }.flatMap {
      last =>
        log.debug("in flatmap")
        if (last < 100 || delivered >= max || maxDeliveredSeq >= toSequenceNr) {
          log.debug("done")
          Future.successful(())
        } else {
          val from = fromSequenceNr + 100
          asyncReplayMessages(processorId, from, toSequenceNr, max - delivered)(replayCallback)
        }
    }
  }

  def readPersistentRepr(item: JMap[String, AttributeValue]): Option[PersistentRepr] = {
    import collection.JavaConverters._
    item.asScala.get(Payload).map {
      payload =>
        val repr = persistentFromByteBuffer(payload.getB)
        val isDeleted = item.get(Deleted).getS == "true"
        val confirmations = item.asScala.get(Confirmations).map {
          ca => ca.getSS.asScala.to[immutable.Seq]
        }.getOrElse(immutable.Seq[String]())
        repr.update(deleted = isDeleted, confirms = confirmations)
    }
  }

  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("at=read-high-sequence processorId={} from={}", processorId, fromSequenceNr)
    Future.sequence {
      Stream.iterate(0L, sequenceShards)(_ + 1).map(l => highSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
          log.debug("highest at=batch-request")
          dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
            resp =>
              log.debug("highest at=batch-response")
              val batchMap = mapBatch(resp.getResponses.get(journalName))
              keys.map {
                key =>
                  Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.flatten.append(Stream(0L)).max
          }
      }
    }.map(_.max).map {
      max =>
        log.debug("at=finish-read-high-sequence high={}", max)
        max
    }
  }


  def getUnprocessedItems(result: BatchGetItemResult): Future[BatchGetItemResult] = {
    if (result.getUnprocessedKeys.size() == 0) Future.successful(result)
    else {
      log.warning("UNPROCESSED ITEMS IN BATCH GET")
      Future.sequence {
        result.getUnprocessedKeys.get(journalName).getKeys.asScala.map {
          k =>
            val g = new GetItemRequest().withTableName(journalName).withKey(k).withConsistentRead(true)
            getUnprocessedItem(g)
        }
      }.map {
        results =>
          val items = result.getResponses.get(journalName)
          results.foreach {
            i => items.add(i.getItem)
          }
          result
      }
    }
  }

  def getUnprocessedItem(g: GetItemRequest, retries: Int = 5): Future[GetItemResult] = {
    if (retries == 0) throw new RuntimeException(s"couldnt get ${g.getKey} after 5 tries")
    dynamo.sendGetItem(g).fallbackTo(getUnprocessedItem(g, retries - 1))
  }


  def mapBatch(b: JList[Item]) = {
    val map = new JHMap[AttributeValue, JMap[String, AttributeValue]]
    b.asScala.foreach {
      item => map.put(item.get(Key), item)
    }
    map
  }


  def readLowestSequenceNr(processorId: String): Future[Long] = {
    log.debug("at=read-lowest-sequence processorId={}", processorId)
    Future.sequence {
      Stream.iterate(0L, sequenceShards)(_ + 1).map(l => lowSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
          dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
            resp =>
              log.debug("at=read-lowest-sequence-batch-response processorId={}", processorId)
              val batchMap = mapBatch(resp.getResponses.get(journalName))
              val min: Long = keys.map {
                key =>
                  Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.flatten.append(Stream(Long.MaxValue)).min
              min
          }
      }
    }.map(_.min).map {
      min =>
        log.debug("at=finish-read-lowest lowest={}", min)
        if (min == Long.MaxValue) 0
        else min
    }
  }

}

class ReplayResequencer(start: Long, end: Long, maxMessages: Long, p: (PersistentRepr) => Unit) extends Actor {

  import scala.collection.mutable

  val log = context.system.log
  private val delayed = mutable.Map.empty[Long, Option[PersistentRepr]]
  private var sequenceDelivered = start - 1
  private var countDelivered = 0
  private var replayDone: Option[ActorRef] = None


  def receive = {
    case (seqnr: Long, Some(message: PersistentRepr)) => resequence(seqnr, Some(message))
    case (seqnr: Long, None) => resequence(seqnr, None)
    case ReplayDone =>
      replayDone = Some(sender)
      if (maxMessages == 0L) signalDone()
  }

  @scala.annotation.tailrec
  private def resequence(seqnr: Long, m: Option[PersistentRepr]) {
    log.debug("at=resequence sequenceNr={} message={}", seqnr, m.isDefined)
    if (seqnr == sequenceDelivered + 1) {
      sequenceDelivered = seqnr
      countDelivered += 1
      m.foreach(p)
      log.debug("at=delivered sequenceNr={} message={}", seqnr, m.isDefined)
    } else {
      delayed += (seqnr -> (m))
    }
    val eo = delayed.remove(sequenceDelivered + 1)
    if (eo.isDefined) resequence(sequenceDelivered + 1, eo.get)
    else if (sequenceDelivered >= end || countDelivered >= maxMessages) {
      signalDone()
    }
  }

  private def signalDone() {
    log.debug("at=resequence-done")
    replayDone.foreach(_ ! ReplayDone)
    self ! PoisonPill
  }

}

case object ReplayDone
