# Migrating from 1.x to 2.x

Apache Pekko Persistence DynamoDB 2.x moves from the AWS SDK for Java v1, which reached
[end of support on 31 December 2025](https://aws.amazon.com/blogs/developer/announcing-end-of-support-for-aws-sdk-for-java-v1-x-on-december-31-2025/),
to the AWS SDK for Java v2. It also moves to Pekko 2.0, so it requires:

- Scala `2.13.x` or `3.3.0+` (Scala 2.12 is no longer supported)
- Pekko `2.0.x+`
- Java `17+`

2.x is not binary compatible with 1.x.

## Data and table schema

**No data migration is needed.** The journal, snapshot store and read journal store items with the
same layout as in 1.x and use the same table schema and indexes, so 2.x reads data written by 1.x
and vice versa. Roll the upgrade out like any other dependency change.

## Configuration

Most applications only need to check the region (see below) and remove any `aws-client-config`
settings that 2.x ignores.

### `region` (new)

The AWS SDK v2 signs every request for a specific region, and does not work the region out from the
`endpoint` host the way v1 did. There is a new `region` setting on `dynamodb-journal`; the snapshot
store and the read journal inherit it by default.

```hocon
my-dynamodb-journal {
  region = "eu-west-1"
}
```

The region is chosen as follows:

1. The `region` setting, if it is set.
2. Otherwise the AWS SDK
   [default region provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html):
   the `AWS_REGION` environment variable, the `aws.region` system property, the shared AWS config
   file, and instance metadata.
3. If that finds nothing and an `endpoint` is set, `us-east-1`. This is meant for DynamoDB Local and
   other test endpoints, where the region does not matter.
4. Otherwise the plugin fails to start, with an error that says how to configure the region.

If you set `endpoint` to a regional AWS endpoint, such as `https://dynamodb.eu-west-1.amazonaws.com`,
make sure the region matches it, either with `region` or through the environment. Otherwise requests
are signed for the wrong region and DynamoDB rejects them. Leaving `endpoint` empty and setting only
the region is the simplest option.

### `aws-client-config`

In 1.x this block was passed straight to the AWS SDK v1 `ClientConfiguration`. In 2.x the keys that
have an AWS SDK v2 equivalent are mapped onto the Netty async HTTP client and the client override
configuration. Every key still defaults to `default`, which keeps the AWS SDK default.

| 1.x key                      | 2.x behaviour                                                      |
|------------------------------|--------------------------------------------------------------------|
| `max-connections`            | Maximum concurrent requests (the Netty client's `maxConcurrency`)  |
| `connection-timeout`         | Connection timeout, in milliseconds                                |
| `socket-timeout`             | Read and write timeout, in milliseconds                            |
| `client-execution-timeout`   | Timeout for a whole API call (`apiCallTimeout`), in milliseconds   |
| `request-timeout`            | Timeout for one attempt (`apiCallAttemptTimeout`), in milliseconds |
| `connection-ttl`             | Maximum lifetime of a pooled connection, in milliseconds           |
| `connection-max-idle-millis` | Maximum idle time of a pooled connection, in milliseconds          |
| `use-reaper`                 | Close idle connections in the background                           |
| `use-tcp-keepalive`          | TCP keep-alive                                                     |
| `max-error-retry`            | Retries made by the AWS SDK itself; see [Retries](#retries)        |
| `proxy-host`, `proxy-port`, `proxy-username`, `proxy-password` | HTTP proxy                       |
| `user-agent`                 | Prefix for the `User-Agent` header                                 |

These keys have no AWS SDK v2 equivalent. They are accepted so that existing configuration still
loads, but they are ignored, and a warning is logged at startup for each one that is set:
`local-address`, `preemptive-basic-proxy-auth`, `protocol`, `proxy-domain`, `proxy-workstation`,
`response-metadata-cache-size`, `signer-override`, `socket-buffer-size-hints`,
`use-expect-continue` and `use-gzip`. To use HTTP instead of HTTPS, put the scheme in `endpoint`.

In 1.x, `max-connections` also sized the thread pool that ran the SDK's requests. The v2 client is
non-blocking and has no such thread pool, so the setting now limits only the number of concurrent
requests. The default is still 50.

### Retries

The plugin retries throttling, server errors (HTTP 500, 502-504) and, new in 2.x, transient
client-side failures (connection errors and per-attempt timeouts) with its own exponential backoff:
up to 10 retries, starting at 1 millisecond and doubling each time. The same backoff now also
applies when a batch read leaves keys unprocessed.

In 1.x the AWS SDK also retried these failures itself, before the plugin did, so the number of
attempts multiplied and a request could stall for a long time under sustained throttling. **In 2.x
the AWS SDK does not retry by default**, and the plugin's backoff is the only retry layer. If you
want the SDK to retry as well, set `aws-client-config.max-error-retry` to the number of retries.

## Code changes

You only need to change code if it uses the AWS SDK types that the plugin exposes, or plugin classes
directly. Code that only uses Pekko Persistence through `PersistentActor`, `EventSourcedBehavior` or
`PersistenceQuery` needs no changes.

### AWS SDK types

Everything that used `com.amazonaws.services.dynamodbv2.model` types now uses the AWS SDK v2
equivalents in `software.amazon.awssdk.services.dynamodb.model`:

- Requests are immutable and built with builders, for example
  `QueryRequest.builder().tableName(...).build()` instead of `new QueryRequest().withTableName(...)`.
- `XxxResult` types are now `XxxResponse`, for example `QueryResult` becomes `QueryResponse`.
  This affects the return types of the `DynamoDBHelper` methods (`query`, `putItem`,
  `batchGetItem`, ...) and `CreatePersistenceIdsIndex.createPersistenceIdsIndex`.
- Getters drop the `get` prefix: `item.get(...).getS` becomes `item.get(...).s`.
- Binary attributes are `SdkBytes` instead of `java.nio.ByteBuffer`.
- `DynamoDBHelper.dynamoDB` is a `DynamoDbAsyncClient` instead of an `AmazonDynamoDBAsync`.

### `schema`

`org.apache.pekko.persistence.dynamodb.journal.schema`, which describes the journal table, was a
`CreateTableRequest` in 1.x and is now a `CreateTableRequest.Builder`, since v2 requests are
immutable. Code that creates the table from it changes from

```scala
val request = schema
  .withTableName("journal")
  .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
client.createTable(request)
```

to

```scala
val request = schema
  .tableName("journal")
  .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
  .build()
client.createTable(request)
```

Each call to `schema` returns a new builder, so it is safe to customise.

### `DynamoDBConfig`

If you implement `DynamoDBConfig` yourself, add the two new members:

- `Region: String` - the region, or an empty string to use the default region provider chain.
- `client: DynamoDBClientConfig` - for example `new DynamoDBClientConfig(config)`, where `config`
  is the plugin's configuration section.

The `ClientConfig` trait, and `DynamoDBClientConfig.config` (the v1 `ClientConfiguration`), have been
removed. `DynamoDBClientConfig` now exposes the parsed settings as `Option` values instead.

### Other API changes

- `DynamoDBHelper.Describe` and its implicit instances, which formatted requests for log messages,
  have been removed.
- The `javax.xml.bind:jaxb-api` dependency is no longer pulled in. Add it to your own build if you
  relied on getting it from this plugin.
