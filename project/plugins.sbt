resolvers += Resolver.bintrayRepo("akka", "snapshots")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.13")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.1")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "1.1.1")
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.10.11",
  "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.4.4"
)
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.0.15")
addDependencyTreePlugin
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "0.5.2")
