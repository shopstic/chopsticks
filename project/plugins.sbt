resolvers += Resolver.bintrayRepo("shopstic", "sbt-plugins")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.5")
addSbtPlugin("com.shopstic" % "sbt-symlink-target" % "0.0.28")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.4.2")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.4")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.2")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.23")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.7.1")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.9.0"
)
