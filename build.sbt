import PekkoDependency.pekkoVersion

name := "pekko-persistence-dynamodb"

scalaVersion := "2.13.7"
crossScalaVersions := Seq("2.12.15", "2.13.7")
crossVersion := CrossVersion.binary

val amzVersion = "1.12.286"
val testcontainersScalaVersion = "0.40.10"

resolvers += "Apache Nexus Snapshots".at("https://repository.apache.org/content/repositories/snapshots/")

ThisBuild / apacheSonatypeProjectProfile := "pekko"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-core" % amzVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % amzVersion,
  "javax.xml.bind" % "jaxb-api" % "2.3.1", // see https://github.com/seek-oss/gradle-aws-plugin/issues/15
  "org.apache.pekko" %% "pekko-persistence" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-query" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0",
  "org.apache.pekko" %% "pekko-persistence-tck" % pekkoVersion % "test",
  "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % "test",
  "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % "test",
  "org.scalatest" %% "scalatest" % "3.1.4" % "test",
  "commons-io" % "commons-io" % "2.11.0" % "test",
  "org.hdrhistogram" % "HdrHistogram" % "2.1.8" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test")

Test / parallelExecution := false
// required by test-containers-scala
Test / fork := true
logBuffered := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
