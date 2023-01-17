name := "akka-persistence-dynamodb"

scalaVersion := "2.13.7"
crossScalaVersions := Seq("2.12.13", "2.12.15", "2.13.5", "2.13.7")
crossVersion := CrossVersion.binary

val akkaVersion = "2.5.29"
val amzVersion = "1.12.286"
val testcontainersScalaVersion = "0.40.10"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-core" % amzVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % amzVersion,
  "javax.xml.bind" % "jaxb-api" % "2.3.1", // see https://github.com/seek-oss/gradle-aws-plugin/issues/15
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0",
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "commons-io" % "commons-io" % "2.4" % "test",
  "org.hdrhistogram" % "HdrHistogram" % "2.1.8" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test")

Test / parallelExecution := false
// required by test-containers-scala
Test / fork := true
logBuffered := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

MetaInfLicenseNoticeCopy.settings
