organization := "com.sclasen"

name := "akka-persistence-dynamodb"

version := "0.1"

scalaVersion := "2.10.3"

parallelExecution in Test := false

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies += "com.sclasen" %% "spray-dynamodb" % "0.2.4" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.0" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.0" % "test,it"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test,it"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test,it"

parallelExecution in Test := false

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
      <url>git@github.com:sclasen/akka-persistence-dynamodb.git</url>
      <connection>scm:git:git@github.com:sclasen/akka-persistence-dynamodb.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sclasen</id>
        <name>Scott Clasen</name>
        <url>http://github.com/sclasen</url>
      </developer>
    </developers>)


publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}


val root = Project("akka-persistence-dynamodb", file(".")).configs(IntegrationTest).settings(Defaults.itSettings:_*)
