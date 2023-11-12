/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

import PekkoDependency.pekkoVersion
import net.bzzt.reproduciblebuilds.ReproducibleBuildsPlugin.reproducibleBuildsCheckResolver

name := "pekko-persistence-dynamodb"

scalaVersion := "2.13.12"
crossScalaVersions := Seq("2.12.18", "2.13.12", "3.3.1")
crossVersion := CrossVersion.binary

val amzVersion = "1.12.571"
val testcontainersScalaVersion = "0.41.0"

ThisBuild / resolvers += Resolver.ApacheMavenSnapshotsRepo
ThisBuild / apacheSonatypeProjectProfile := "pekko"
ThisBuild / versionScheme := Some(VersionScheme.SemVerSpec)
sourceDistName := "apache-pekko-persistence-dynamodb"
sourceDistIncubating := true

ThisBuild / reproducibleBuildsCheckResolver := Resolver.ApacheMavenStagingRepo

inThisBuild(Def.settings(
  onLoad in Global := {
    sLog.value.info(
      s"Building Pekko Persistence DynamoDB ${version.value} against Pekko ${PekkoDependency.pekkoVersion} on Scala ${(ThisBuild / scalaVersion).value}")
    (onLoad in Global).value
  }))

commands := commands.value.filterNot { command =>
  command.nameOption.exists { name =>
    name.contains("sonatypeRelease") || name.contains("sonatypeBundleRelease")
  }
}

Test / unmanagedSourceDirectories ++= {
  if (scalaVersion.value.startsWith("2.")) {
    Seq(
      (LocalRootProject / baseDirectory).value / "src" / "test" / "scala-2")
  } else {
    Seq.empty
  }
}

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-core" % amzVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % amzVersion,
  "javax.xml.bind" % "jaxb-api" % "2.3.1", // see https://github.com/seek-oss/gradle-aws-plugin/issues/15
  "org.apache.pekko" %% "pekko-persistence" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-query" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-tck" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "commons-io" % "commons-io" % "2.14.0" % Test,
  "org.hdrhistogram" % "HdrHistogram" % "2.1.8" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % Test)

scalacOptions ++= Seq("-deprecation", "-feature")
Test / parallelExecution := false
// required by test-containers-scala
Test / fork := true
logBuffered := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

enablePlugins(ReproducibleBuildsPlugin)
enablePlugins(DockerComposePlugin)
