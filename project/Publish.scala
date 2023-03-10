/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package org.apache.pekko

import sbt._
import sbt.Keys._

import java.io.File
import org.mdedetrich.apache.sonatype.SonatypeApachePlugin
import SonatypeApachePlugin.autoImport.apacheSonatypeDisclaimerFile

object Publish extends AutoPlugin {

  override def trigger = allRequirements
  override def requires = SonatypeApachePlugin

  override lazy val projectSettings = Seq(
    crossPaths := false,
    homepage := Some(url("https://github.com/apache/incubator-pekko-persistence-dynamodb")),
    publishTo := {
      val nexus = s"https://${apacheBaseRepo}/"
      if (isSnapshot.value) Some("snapshots".at(nexus + "content/repositories/snapshots"))
      else Some("releases".at(nexus + "service/local/staging/deploy/maven2"))
    },
    developers += Developer("contributors",
      "Contributors",
      "dev@pekko.apache.org",
      url("https://github.com/apache/incubator-pekko-persistence-dynamodb/graphs/contributors")),
    apacheSonatypeDisclaimerFile := Some((LocalRootProject / baseDirectory).value / "DISCLAIMER"))

  private val apacheBaseRepo = "repository.apache.org"
}
