# Apache Pekko DynamoDB Persistence Plugin

A replicated Pekko Persistence journal backed by
[Amazon DynamoDB](https://aws.amazon.com/dynamodb/).

- This plugin implements both a journal as well as a snapshot store,
- This includes a Pekko Persistence Query plugin. However, this requires an additional GSI for efficient usage.

Supported versions:
- Scala: `2.12.x`, `2.13.x`, `3.3.0+`
- Pekko: `1.0.x+`
- Java: `1.8+`

[![Build Status](https://github.com/apache/pekko-persistence-dynamodb/actions/workflows/check-build-test.yml/badge.svg?branch=main)](https://github.com/apache/pekko-persistence-dynamodb/actions)

## Installation

This plugin is published to the Maven Central repository with the following names:

~~~
<dependency>
    <groupId>org.apache.pekko</groupId>
    <artifactId>pekko-persistence-dynamodb_2.13</artifactId>
    <version>1.1.0</version>
</dependency>
~~~

or for sbt users:

```sbt
libraryDependencies += "org.apache.pekko" %% "pekko-persistence-dynamodb" % "1.1.0"
```

Snapshot versions are available.
- To work out a version to use, see https://repository.apache.org/content/groups/snapshots/org/apache/pekko/pekko-persistence-dynamodb_2.13/
- you will need to add a resolver set to `https://repository.apache.org/content/groups/snapshots`
- in sbt 1.9.0+, you can add `resolvers += Resolver.ApacheMavenSnapshotsRepo`

## Configuration

### Journal
~~~
pekko.persistence.journal.plugin = "my-dynamodb-journal"

my-dynamodb-journal = ${dynamodb-journal} # include the default settings
my-dynamodb-journal {                     # and add some overrides
    journal-table =  <the name of the table to be used>
    journal-name =  <prefix to be used for all keys stored by this plugin>
    aws-access-key-id =  <your key>
    aws-secret-access-key =  <your secret>
    endpoint =  "https://dynamodb.us-east-1.amazonaws.com" # or where your deployment is
}
~~~

For details on the endpoint URL please refer to the [DynamoDB documentation](https://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region). There are many more settings that can be used for fine-tuning and adapting this journal plugin to your use-case, please refer to the [reference.conf](https://github.com/apache/pekko-persistence-dynamodb/blob/main/src/main/resources/reference.conf) file.

Before you can use these settings you will have to create a table, e.g. using the AWS console, with the following schema:

  * a hash key of type String with name `par`
  * a sort key of type Number with name `num`

### Snapshot store
contributed by [@joost-de-vries](https://github.com/joost-de-vries)

~~~
pekko.persistence.snapshot-store.plugin = "my-dynamodb-snapshot-store"

my-dynamodb-snapshot-store = ${dynamodb-snapshot-store} # include the default settings
my-dynamodb-snapshot-store {                     # and add some overrides
    snapshot-table =  <the name of the table to be used>
    journal-name =  <prefix to be used for all keys stored by this plugin>
    aws-access-key-id =  <your key, default is the same as journal>
    aws-secret-access-key =  <your secret, default is the same as journal>
    endpoint =  "https://dynamodb.us-east-1.amazonaws.com" # or where your deployment is, default is the same as journal
}
~~~

The table to create for snapshot storage has the schema:

* a hash key of type String with name `par`
* a sort key of type Number with name `seq`
* a sort key of type Number with name `ts`
* a local secondary index with name `ts-idx` that is an index on the combination of `par` and `ts`

The DynamoDB item of a snapshot [can be 400 kB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-items). Using a binary serialisation format like ProtoBuf or Kryo will use that space most effectively.

### Read journal (Pekko persistence query)
contributed by [@joost-de-vries](https://github.com/joost-de-vries)

See `CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest` how to create the Global Secondary Index that is required to query currentPersistenceIds
~~~
dynamodb-read-journal {
  # The name of the Global Secondary Index that is used to query currentPersistenceIds
  # see CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest
  # persistence-ids-index-name: "persistence-ids-idx"
}
~~~

## Storage Semantics

DynamoDB only offers consistency guarantees for a single storage item—which corresponds to one event in the case of this Pekko Persistence plugin. This means that any single event is either written to the journal (and thereby visible to later replays) or it is not. This plugin supports atomic multi-event batches nevertheless, by marking the contained events such that partial replay can be avoided (see the `idx` and `cnt` attributes in the storage format description below). Consider the following actions of a PersistentActor:

```scala
val events = List(<some events>)
if (atomic) {
  persistAll(events)(handler)
else {
  for (event <- events) persist(event)(handler)
}
```

In the first case a recovery will only ever see all of the events or none of them. This is also true if recovery is requested with an upper limit on the sequence number to be recovered to or a limit on the number of events to be replayed; the event count limit is applied before removing incomplete batch writes which means that the actual count of events received at the actor may be lower than the requested limit even if further events are available.

In the second case each event is treated in isolation and may or may not be replayed depending on whether it was persisted successfully or not.

## Performance Considerations

This plugin uses the AWS Java SDK which means that the number of requests that can be made concurrently is limited by the number of connections to DynamoDB and by the number of threads in the thread-pool that is used by the AWS HTTP client. The default setting is 50 connections which for a deployment that is used from the same EC2 region allows roughly 5000 requests per second (where every persisted event batch is roughly one request). If a single ActorSystem needs to persist more than this number of events per second then you may want to tune the parameter

~~~
my-dynamodb-journal.aws-client-config.max-connections = <your value here>
~~~

Changing this number changes both the number of concurrent connections and the used thread-pool size.

## Retry behavior

This plugin uses exponential backoff when plausible and retriable errors from DynamoDB occur. This includes network glitches (50x; since Pekko 1.1.0) and throughput exceptions (400; extended in Pekko 1.1.0).

The backoff strategy is very simple. 
- There are a maximum of 10 retries.
- The first time it retries, it takes 1 millisecond.
- That time is doubled with every retry.

This means that the last waiting time would be about half a second, and if responses would be immediate, the total retrial process would take about 1 second. In practice, response time would be more than 0 of course.


## Compatibility with Akka versions

pekko-persistence-dynamodb is derived from [akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb) v1.3.0.

Anyone migrating from using akka-persistence-dynamodb should first upgrade to akka-persistence-dynamodb v1.3.0.

## Plugin Development

### Dev Setup

* Run `./docker-compose up` to download and start [Localstack](https://github.com/localstack/localstack/).
* Make sure that env variables from .env.test are exported `source .env.test`
* Now you are all set for running the test suite from `sbt`.
* In order to stop the DynamoDB and clean up execute `./docker-compose down`.

Please also read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

### DynamoDB table structure discussion

The structure for journal storage in dynamodb has evolved over iterations of performance tuning. Most of these lessons were learned in creating the eventsourced dynamodb journal, but apply here as well.

##### Naive structure

When initially modelling journal storage in dynamo, it seems natural to use a simple structure similar to this

```
persistenceId : S : HashKey
sequenceNr    : N : RangeKey
payload       : B
```

This maps very well to the operations a journal needs to solve.

```
writeMessage      -> PutItem
deleteMessage     -> DeleteItem
replayMessages    -> Query by persistenceId, conditions and ordered by sequenceNr, ascending
highCounter       -> Query by persistenceId, conditions and ordered by sequenceNr, descending limit 1
```

However this layout suffers from scalability problems. Since the hash key is used to locate the data storage node, all writes for a single processor will go to the same DynamoDB node, which limits throughput and invites throttling, no matter the level of throughput provisioned for a table—the hash key just gets too hot. Also this limits replay throughput since you have to step through a sequence of queries, where you use the last processed item in query N for query N+1.

##### Higher throughput structure

With the following abbreviations:

~~~
P -> PersistentRepr
SH -> SequenceHigh
SL -> SequenceLow
~~~

we model PersistentRepr storage as

~~~
par = <journalName>-P-<persistenceId>-<sequenceNr / 100> : S : HashKey
num = <sequenceNr % 100>                                 : N : RangeKey
pay = <payload>                                          : B
idx = <atomic write batch index>                         : N (possibly absent)
cnt = <atomic write batch max index>                     : N (possibly absent)
~~~

High Sequence Numbers

~~~
par = <journalName>-SH-<persistenceId>-<(sequenceNr / 100) % sequenceShards> : S : HashKey
num = 0                                                                      : N : RangeKey
seq = <sequenceNr rounded down to nearest multiple of 100>                   : N
~~~

Low Sequence Numbers

~~~
par = <journalName>-SL-<persistenceId>-<(sequenceNr / 100) % sequenceShards> : S : HashKey
num = 0                                                                      : N : RangeKey
seq = <sequenceNr, not rounded>                                              : N
~~~

This is somewhat more difficult to code, but offers higher throughput possibilities. Notice that the items that hold the high and low sequence are sharded, rather than using a single item to store the counter. If we only used a single item, we would suffer from the same hot key problems as our first structure.

When writing an item we typically do not touch the high sequence number storage, only when writing an item with sort key `0` is this done. This implies that reading the highest sequence number will need to first query the sequence shards for the highest multiple of 100 and then send a `Query` for the corresponding P entry’s hash key to find the highest stored sort key number.

## Building from Source

### Prerequisites
- Make sure you have installed a Java Development Kit (JDK) version 8 or later.
- Make sure you have [sbt](https://www.scala-sbt.org/) installed and using this JDK.
- [Graphviz](https://graphviz.gitlab.io/download/) is needed for the scaladoc generation build task, which is part of the release.

### Running the Build
- Open a command window and change directory to your preferred base directory
- Use git to clone the [repo](https://github.com/apache/pekko-persistence-dynamodb) or download a source release from https://pekko.apache.org (and unzip or untar it, as appropriate)
- Change directory to the directory where you installed the source (you should have a file called `build.sbt` in this directory)
- `sbt compile` compiles the main source for project default version of Scala (2.13)
    - `sbt +compile` will compile for all supported versions of Scala
- `sbt test` will compile the code and run the unit tests
- `sbt testQuick` similar to test but when repeated in shell mode will only run failing tests
- `sbt package` will build the jar
    - the jar will be built to `target` directory
- `sbt publishLocal` will push the jars to your local Apache Ivy repository
- `sbt publishM2` will push the jars to your local Apache Maven repository
- `sbt doc` will build the Javadocs for all the modules and load them to one place (may require Graphviz, see Prerequisites above)
    - the `index.html` file will appear in `target/api/`
- `sbt sourceDistGenerate` will generate source release to `target/dist/`
- The version number that appears in filenames and docs is derived, by default. The derived version contains the most git commit id or the date/time (if the directory is not under git control).
    - You can set the version number explicitly when running sbt commands
        - eg `sbt "set ThisBuild / version := \"1.0.0\"; sourceDistGenerate"`
    - Or you can add a file called `version.sbt` to the same directory that has the `build.sbt` containing something like
        - `ThisBuild / version := "1.0.0"`

## Community

There are several ways to interact with the Pekko community:

- [GitHub discussions](https://github.com/apache/pekko-persistence-dynamodb/discussions): for questions and general discussion.
- [Pekko dev mailing list](https://lists.apache.org/list.html?dev@pekko.apache.org): for Pekko development discussions.
- [Pekko users mailing list](https://lists.apache.org/list.html?users@pekko.apache.org): for Pekko user discussions.
- [GitHub issues](https://github.com/apache/pekko-persistence-dynamodb/issues): for bug reports and feature requests. Please search the existing issues before creating new ones. If you are unsure whether you have found a bug, consider asking in GitHub discussions or the mailing list first.

## Credits

- Initial development was done by [Scott Clasen](https://github.com/sclasen/akka-persistence-dynamodb).
- Update to Akka 2.4 and further development up to version 1.0 was kindly sponsored by [Zynga Inc.](https://www.zynga.com/).
- The snapshot store and readjournal were contributed by [Joost de Vries](https://github.com/joost-de-vries)
- [Corey O'Connor](https://dogheadbone.com/)
- Lightbend team
- Apache Pekko Community
- Ryan Means
- Jean-Luc Deprez
- Michal Janousek

## Code of Conduct

Apache Pekko is governed by the [Apache code of conduct](https://www.apache.org/foundation/policies/conduct.html). By participating in this project you agree to abide by its terms.

## License

Apache Pekko DynamoDB Persistence Plugin is available under the Apache License, version 2.0. See [LICENSE](LICENSE) file for details.
