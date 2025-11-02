/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

import com.github.pjfanning.pekkobuild._
import net.bzzt.reproduciblebuilds.ReproducibleBuildsPlugin.reproducibleBuildsCheckResolver

val amzVersion = "1.12.792"
val testcontainersScalaVersion = "0.43.6"

ThisBuild / versionScheme := Some(VersionScheme.SemVerSpec)
sourceDistName := "apache-pekko-persistence-dynamodb"
sourceDistIncubating := false

ThisBuild / reproducibleBuildsCheckResolver := Resolver.ApacheMavenStagingRepo

Test / unmanagedSourceDirectories ++= {
  if (scalaVersion.value.startsWith("2.")) {
    Seq(
      (LocalRootProject / baseDirectory).value / "src" / "test" / "scala-2")
  } else {
    Seq.empty
  }
}

lazy val root = Project(
  id = "pekko-persistence-dynamodb",
  base = file("."))
  .enablePlugins(ReproducibleBuildsPlugin)
  .addPekkoModuleDependency("pekko-persistence", "", PekkoCoreDependency.default)
  .addPekkoModuleDependency("pekko-persistence-query", "", PekkoCoreDependency.default)
  .addPekkoModuleDependency("pekko-stream", "", PekkoCoreDependency.default)
  .addPekkoModuleDependency("pekko-persistence-tck", "test", PekkoCoreDependency.default)
  .addPekkoModuleDependency("pekko-testkit", "test", PekkoCoreDependency.default)
  .addPekkoModuleDependency("pekko-stream-testkit", "test", PekkoCoreDependency.default)
  .settings(
    name := "pekko-persistence-dynamodb",
    scalaVersion := "2.13.17",
    crossScalaVersions := Seq("2.13.17", "3.3.7"),
    crossVersion := CrossVersion.binary,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-core" % amzVersion,
      "com.amazonaws" % "aws-java-sdk-dynamodb" % amzVersion,
      "javax.xml.bind" % "jaxb-api" % "2.3.1", // see https://github.com/seek-oss/gradle-aws-plugin/issues/15
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "commons-io" % "commons-io" % "2.20.0" % Test,
      "org.hdrhistogram" % "HdrHistogram" % "2.2.2" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % Test),
    scalacOptions ++= Seq("-deprecation", "-feature"),
    Test / parallelExecution := false,
    // required by test-containers-scala
    Test / fork := true,
    logBuffered := false,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    onLoad := {
      sLog.value.info(
        s"Building Pekko Persistence DynamoDB ${version.value} against Pekko ${PekkoCoreDependency.version} on Scala ${scalaVersion.value}")
      onLoad.value
    }
  )
