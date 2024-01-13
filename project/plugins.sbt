addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.3")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.21")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.1.6")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13",
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.1"
)
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.2")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addDependencyTreePlugin
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.12")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
