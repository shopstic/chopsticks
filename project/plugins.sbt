addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.6")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.4")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.7")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.31")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.8.4")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.10.3"
)
