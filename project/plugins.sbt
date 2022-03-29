addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.18")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.1.3")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.10",
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.5.1"
)
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addDependencyTreePlugin
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.12")
