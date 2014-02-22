organization := "com.sclasen"

name := "akka-persistence-dynamodb"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

parallelExecution in Test := false

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies += "com.sclasen" %% "spray-dynamodb" % "0.2.4-SNAPSHOT" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.0-RC4" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.0-RC4" % "test,it"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test,it"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test,it"

parallelExecution in Test := false

publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}



