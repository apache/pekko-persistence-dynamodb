resolvers += "Typesafe repository".at("https://repo.typesafe.com/typesafe/releases/")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.9.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.7")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("org.mdedetrich" % "sbt-apache-sonatype" % "0.1.5")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")
