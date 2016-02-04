organization := "com.typesafe.akka"
name := "akka-persistence-dynamodb"
version := "0.9-SNAPSHOT"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.2-RC2"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-core" % "1.10.50",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.50",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

parallelExecution in Test := false
logBuffered := false
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

pomExtra := (
  <url>http://github.com/sclasen/akka-persistence-dynamodb</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:akka/akka-persistence-dynamodb.git</url>
      <connection>scm:git:git@github.com:akka/akka-persistence-dynamodb.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sclasen</id>
        <name>Scott Clasen</name>
        <url>http://github.com/sclasen</url>
      </developer>
      <developer>
        <id>akka</id>
        <name>The Akka Team</name>
        <url>http://github.com/akka</url>
      </developer>
    </developers>)


publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

