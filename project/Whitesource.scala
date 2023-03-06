/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._
import com.typesafe.sbt.SbtGit.GitKeys._

object Whitesource extends AutoPlugin {
  override def requires = WhiteSourcePlugin

  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName := {
      val projectName = (LocalRootProject / moduleName).value.replace("-root", "")
      projectName + "-" + (if (isSnapshot.value)
                             if (gitCurrentBranch.value == "main") "main"
                             else "adhoc"
                           else
                             CrossVersion
                               .partialVersion((LocalRootProject / version).value)
                               .map { case (major, minor) => s"$major.$minor-stable" }
                               .getOrElse("adhoc"))
    },
    whitesourceForceCheckAllDependencies := true,
    whitesourceFailOnError := true)
}
