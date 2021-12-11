addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.3")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.4")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.16")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.5")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.1.1")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.6",
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.5.1"
)
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.2")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addDependencyTreePlugin
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.10")
