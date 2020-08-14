addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.6")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.9")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.34")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.8.4")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.10.3"
)
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")
