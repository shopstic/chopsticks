import sbt._

//noinspection ScalaUnusedSymbol,TypeAnnotation
object Dependencies {
  val SCALA_VERSION = "2.12.8"
  val AKKA_VERSION = "2.5.23"

  val akkaSlf4jDeps = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION
  )

  val akkaStreamDeps = Seq(
    "akka-stream",
    "akka-stream-typed"
  ).map { p =>
    "com.typesafe.akka" %% p % AKKA_VERSION
  }

  val akkaActorDeps = Seq(
    "akka-actor",
    "akka-actor-typed"
  ).map { p =>
    "com.typesafe.akka" %% p % AKKA_VERSION
  }

  val zioDeps = Seq(
    "org.scalaz" %% "scalaz-zio" % "1.0-RC5"
  )

  val squantsDeps = Seq(
    "org.typelevel" %% "squants" % "1.4.0"
  )

  val kamonCoreDeps = Seq(
    "io.kamon" %% "kamon-core" % "1.1.6"
  )

  val kamonProdDeps = Seq(
    "io.kamon" %% "kamon-prometheus" % "1.1.1",
    "io.kamon" %% "kamon-system-metrics" % "1.0.1"
  )

  val akkaGrpcRuntimeDeps = Seq(
    "com.lightbend.akka.grpc" %% "akka-grpc-runtime" % "0.6.1"
  )

  val catsCoreDeps = Seq(
    "org.typelevel" %% "cats-core" % "1.6.1"
  )

  val pureconfigDeps = Seq("pureconfig", "pureconfig-akka")
    .map(p => "com.github.pureconfig" %% p % "0.11.0")

  val akkaTestDeps = Seq("akka-testkit", "akka-stream-testkit", "akka-actor-testkit-typed")
    .map(p => "com.typesafe.akka" %% p % AKKA_VERSION)

  val quicklensDeps = Seq(
    "com.softwaremill.quicklens" %% "quicklens" % "1.4.12"
  )

  val betterfilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.7.1"
  )

  val nameofDeps = Seq(
    "com.github.dwickern" %% "scala-nameof" % "1.0.3" % "provided"
  )

  val loggingDeps = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.26",
    "org.slf4j" % "jul-to-slf4j" % "1.7.26",
    "org.codehaus.janino" % "janino" % "3.0.12"
  )

  val scalatestDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.0.8",
    "org.scalatest" %% "scalatest" % "3.0.8",
    "org.scalacheck" %% "scalacheck" % "1.14.0",
    "org.scalamock" %% "scalamock" % "4.2.0"
  )

  val hamstersDeps = Seq(
    "io.github.scala-hamsters" %% "hamsters" % "3.1.0"
  )

  val rocksdbDeps = Seq(
    "org.rocksdb" % "rocksdbjni" % "6.0.1"
  )

  val nettyDeps = Seq(
    "io.netty" % "netty-all" % "4.1.36.Final"
  )

  val chSmppDeps = Seq(
    "com.fizzed" % "ch-smpp" % "6.0.0-netty4-beta-3"
  )

  val shopsticDeps = Seq(
    "gr" %% "stream" % "0.0.6",
    "gr" %% "app-base" % "0.0.5"
  )

  val enumeratumDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.5.13"
  )

  val refinedDeps = Seq(
    "eu.timepit" %% "refined" % "0.9.8",
    "eu.timepit" %% "refined-pureconfig" % "0.9.8"
  )
}
