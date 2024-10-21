/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package org.apache.pekko

import sbt._
import sbt.Keys._
import org.mdedetrich.apache.sonatype.ApacheSonatypePlugin
import sbtdynver.DynVerPlugin
import sbtdynver.DynVerPlugin.autoImport.dynverSonatypeSnapshots

object Publish extends AutoPlugin {

  override def trigger = allRequirements
  override def requires = ApacheSonatypePlugin && DynVerPlugin

  override lazy val projectSettings = Seq(
    homepage := Some(url("https://github.com/apache/pekko-persistence-dynamodb")),
    developers += Developer("contributors",
      "Contributors",
      "dev@pekko.apache.org",
      url("https://github.com/apache/pekko-persistence-dynamodb/graphs/contributors")))

  override lazy val buildSettings = Seq(
    dynverSonatypeSnapshots := true)
}
