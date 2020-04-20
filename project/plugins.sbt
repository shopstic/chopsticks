addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.6")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.1")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.7")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.27")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.7.3")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.7"
)
