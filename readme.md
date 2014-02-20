### dev setup

* install [forego](https://github.com/ddollar/forego) if you dont have it.

* run `bin/get-dynamodb-local`

this downloads and unpacks the dynamodb local to a subdir of ./dynamodb-local

* `cp .env.sample .env`

* make sure the DYNAMODB_RELEASE var in .env matches the date of the distro that was placed in ./dynamodb-local

* `forego start`

This starts the local dynamodb instance

In another shell

* forego run sbt test

### dynamodb table structure discussion

the structure for journal storage in dynamodb has evolved over iterations of performance tuning. Most of these lessons were learned
in creating the eventsourced dynamodb journal, but apply here as well.

##### naiive structure

When initially modelling journal storage in dynamo, it seems natural to use a simple structure similar to this

```
processorId  : S : HashKey
sequenceNr   : N : RangeKey
deleted      : S
confirmations: SS
payload      : B
```

This maps very well to the operations a journal needs to solve.

```
writeMessage      -> PutItem
writeConfirmation -> UpdateItem with set add
deleteMessage     -> UpdateItem (mark deleted) or DeleteItem (permanent delete)
replayMessages    -> Query by processorId, conditions and ordered by sequenceNr, ascending
highCounter       -> Query by processorId, conditions and ordered by sequenceNr, descending limit 1
```

However this layout suffers from scalability problems. Since the hash key is used to locate the data storage node, all writes for a
single processor will go to the same dynamodb node, which limits throughput and invites throttling, no matter the level of throughput provisioned
for a table. The hash key just gets too hot. Also this limits replay throughput since you have to step through a sequence of queries, where
you use the last processed item in query N for query N+1.

##### higher throughput structure.

```
P -> PersistentRepr
SH -> SequenceHigh
SL -> SequenceLow

"P"-processorId-sequenceNr  : S : HashKey
deleted                     : S
confirmations               : SS
payload                     : B

or

"SH"-processorId-(sequenceNr % sequenceShards): S : HashKey
sequenceNr                                    : N

"SL"-processorId-(sequenceNr % sequenceShards): S : HashKey
sequenceNr                                    : N
```

This is somewhat more difficult to code, but offers higher throughput possibilities. Notice that the items that hold the high counter are sharded,
rather than using a single item to store the counter. If we only used a single item, we would suffer from the same hot key problems as our
first structure.

```
writeMessage      -> BatchPutItem (P and SH)
writeConfirmation -> UpdateItem with set add
deleteMessage     -> BatchPutItem (mark deleted, set SL) or BatchPutItem (permanent delete, set SL)
replayMessages    -> Parallel BatchGet all P items for the processor
highCounter       -> Parallel BatchGet all SH shards (keys are known) , find max.
lowCounter        -> Parallel BatchGet all SL shards (keys are known) , find min.
```


