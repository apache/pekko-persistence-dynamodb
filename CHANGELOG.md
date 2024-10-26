# Apache Pekko Persistence DynamoDB Releases

## v1.1.0

### Changes

* We have made some minor dependency upgrades, notably the AWS libs (latest available [AWS SDK for Java 1.x](https://github.com/aws/aws-sdk-java) lib at release time, October 2024).
* Built with Pekko 1.1.x. With this release, it is recommended to use the latest Pekko 1.1.x libs but in theory, the 1.0.x libs should work too.
* Retry on 50x responses from AWS. (#11) 
* Add `setSelect(ALL_ATTRIBUTES)` to retrieve snapshots. (#12)
* Protect against doing requests for snapshots that are over 400KB. (#13)

## v1.0.0

Pekko Persistence DynamoDB 1.0.0 is based on Akka Persistence DynamoDB 1.1.2. Pekko came about as a result of Lightbend’s decision to make future Akka releases under a [Business Software License](https://www.lightbend.com/blog/why-we-are-changing-the-license-for-akka), a license that is not compatible with Open Source usage.

Apache Pekko has changed the package names, among other changes. Config names have changed to use `pekko` instead of `akka` in their names. Users switching from Akka to Pekko should read our [Migration Guide](https://pekko.apache.org/docs/pekko/1.0/project/migration-guides.html).

Generally, we have tried to make it as easy as possible to switch existing Akka based projects over to using Pekko.

We have gone through the code base and have tried to properly acknowledge all third party source code in the Apache Pekko code base. If anyone believes that there are any instances of third party source code that is not properly acknowledged, please get in touch.

### Changes

* We have made some minor dependency upgrades, notably the AWS libs ([PR](https://github.com/apache/pekko-persistence-dynamodb/pull/84)).
* We added support for Scala 3.3.0+.
