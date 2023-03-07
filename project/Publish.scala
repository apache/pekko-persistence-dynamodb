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
import sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction
import org.mdedetrich.apache.sonatype.SonatypeApachePlugin
import SonatypeApachePlugin.autoImport.apacheSonatypeDisclaimerFile

object Publish extends AutoPlugin {

  val defaultPublishTo = settingKey[File]("Default publish directory")

  override def trigger = allRequirements
  override def requires = sbtrelease.ReleasePlugin && SonatypeApachePlugin

  override lazy val projectSettings = Seq(
    crossPaths := false,
    pomExtra := pekkoPomExtra,
    credentials ++= apacheNexusCredentials,
    homepage := Some(url("https://github.com/apache/incubator-pekko-persistence-dynamodb")),
    pomIncludeRepository := { x => false },
    defaultPublishTo := crossTarget.value / "repository",
    apacheSonatypeDisclaimerFile := Some((LocalRootProject / baseDirectory).value / "DISCLAIMER"))

  def pekkoPomExtra = {
    <developers>
      <developer>
        <id>contributors</id>
        <name>Contributors</name>
        <email>dev@pekko.apache.org</email>
        <url>https://github.com/apache/incubator-pekko-persistence-dynamodb/graphs/contributors</url>
      </developer>
    </developers>
  }

  private val apacheBaseRepo = "repository.apache.org"

  private def apacheNexusCredentials: Seq[Credentials] =
    (sys.env.get("NEXUS_USER"), sys.env.get("NEXUS_PW")) match {
      case (Some(user), Some(password)) =>
        Seq(Credentials("Sonatype Nexus Repository Manager", apacheBaseRepo, user, password))
      case _ =>
        Seq.empty
    }
}
