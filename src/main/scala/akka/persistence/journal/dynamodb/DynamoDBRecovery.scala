package akka.persistence.journal.dynamodb

import DynamoDBJournal._
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

  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logging {
    if (fromSequenceNr > toSequenceNr) return Future.successful(())
    var delivered = 0L
    var maxDeliveredSeq = 0L
    getReplayBatch(processorId, fromSequenceNr).map {
      replayBatch =>
        replayBatch.keys.foreach {
          case (sequenceNr, key) =>
            val k = key.get(Key)
            Option(replayBatch.batch.get(k)).map {
              item =>
                val repr = readPersistentRepr(item)
                repr.foreach {
                  r =>
                    if (delivered < max && maxDeliveredSeq < toSequenceNr) {
                      replayCallback(r)
                      delivered += 1
                      maxDeliveredSeq = r.sequenceNr
                      log.debug("in=replay at=deliver {} {}", processorId, sequenceNr)
                    }
                }
            }
        }
        replayBatch.batch.size()
    }.flatMap {
      last =>
        if (last < maxDynamoBatchGet * replayParallelism || delivered >= max || maxDeliveredSeq >= toSequenceNr) {
          Future.successful(())
        } else {
          val from = fromSequenceNr + maxDynamoBatchGet * replayParallelism
          asyncReplayMessages(processorId, from, toSequenceNr, max - delivered)(replayCallback)
        }
    }
  }

  case class ReplayBatch(keys: Stream[(Long, Item)], batch: JMap[AttributeValue, Item])

  def getReplayBatch(processorId: String, fromSequenceNr: Long): Future[ReplayBatch] = {
    val batchKeys = Stream.iterate(fromSequenceNr, maxDynamoBatchGet * replayParallelism)(_ + 1).map(s => s -> fields(Key -> messageKey(processorId, s)))
    //there will be replayParallelism number of gets
    val gets = batchKeys.grouped(maxDynamoBatchGet).map {
      keys =>
        val ka = new KeysAndAttributes().withKeys(keys.map(_._2).asJava).withConsistentRead(true).withAttributesToGet(Key, Payload, Deleted, Confirmations)
        val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
        dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
          result => mapBatch(result.getResponses.get(journalName))
        }
    }

    Future.sequence(gets).map {
      responses =>
        val batch = responses.foldLeft(mapBatch(Collections.emptyList())) {
          case (all, one) =>
            all.putAll(one)
            all
        }
        ReplayBatch(batchKeys, batch)
    }
  }

  def readPersistentRepr(item: JMap[String, AttributeValue]): Option[PersistentRepr] = {
    Option(item.get(Payload)).map {
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
    log.debug("in=read-highest processorId={} from={}", processorId, fromSequenceNr)
    Future.sequence {
      Stream.iterate(0L, sequenceShards)(_ + 1).map(l => highSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = new BatchGetItemRequest().withRequestItems(Collections.singletonMap(journalName, ka))
          log.debug("in=read-highest at=batch-request")
          dynamo.sendBatchGetItem(get).flatMap(getUnprocessedItems).map {
            resp =>
              log.debug("in=read-highest at=batch-response")
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

  def getUnprocessedItems(result: BatchGetItemResult): Future[BatchGetItemResult] = {
    if (result.getUnprocessedKeys.size() == 0) Future.successful(result)
    else {
      log.warning("at=get-unprocessed-items")
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
    if (retries == 0) Future.failed(new RuntimeException(s"couldnt get ${g.getKey} after 5 tries"))
    dynamo.sendGetItem(g).fallbackTo{
      val sleep = (6 - retries) * 100
      Thread.sleep(sleep)
      getUnprocessedItem(g, retries - 1)
    }
  }

  def mapBatch(b: JList[Item]): JMap[AttributeValue, Item] = {
    val map = new JHMap[AttributeValue, JMap[String, AttributeValue]]
    b.asScala.foreach {
      item => map.put(item.get(Key), item)
    }
    map
  }

}


