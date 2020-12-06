resolvers += Resolver.bintrayRepo("akka", "snapshots")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.6.1")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.13")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.0-RC3")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "1.0.2-30-8780b706")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.10.9"
)
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")
addDependencyTreePlugin
