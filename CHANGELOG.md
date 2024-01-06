# Apache Pekko Persistence DynamoDB Releases

## v1.0.0

Pekko Persistence DynamoDB 1.0.0 is based on Akka Persistence DynamoDB 1.1.2. Pekko came about as a result of Lightbend’s decision to make future Akka releases under a [Business Software License](https://www.lightbend.com/blog/why-we-are-changing-the-license-for-akka), a license that is not compatible with Open Source usage.

Apache Pekko has changed the package names, among other changes. Config names have changed to use `pekko` instead of `akka` in their names. Users switching from Akka to Pekko should read our [Migration Guide](https://pekko.apache.org/docs/pekko/current/project/migration-guides.html).

Generally, we have tried to make it as easy as possible to switch existing Akka based projects over to using Pekko.

We have gone through the code base and have tried to properly acknowledge all third party source code in the Apache Pekko code base. If anyone believes that there are any instances of third party source code that is not properly acknowledged, please get in touch.

### Changes

* We have made some minor dependency upgrades, notably the AWS libs ([PR](https://github.com/apache/incubator-pekko-persistence-dynamodb/pull/84)).
* We added support for Scala 3.3.0+.
