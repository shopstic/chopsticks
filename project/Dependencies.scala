import sbt._
import scalapb.compiler.Version.scalapbVersion

//noinspection ScalaUnusedSymbol,TypeAnnotation
object Dependencies {
  val SCALA_VERSION = "2.13.1"
  val AKKA_VERSION = "2.6.4"
  val AKKA_HTTP_VERSION = "10.1.10"
  val ZIO_VERSION = "1.0.0-RC18-2"

  val akkaSlf4jDeps = Seq(
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION
  )

  val akkaStreamDeps = Seq(
    "akka-stream",
    "akka-stream-typed"
  ).map { p => "com.typesafe.akka" %% p % AKKA_VERSION }

  val akkaActorDeps = Seq(
    "akka-actor",
    "akka-actor-typed"
  ).map { p => "com.typesafe.akka" %% p % AKKA_VERSION }

  val akkaDiscoveryOverrideDeps = Seq(
    "akka-discovery"
  ).map { p => "com.typesafe.akka" %% p % AKKA_VERSION }

  val akkaHttpDeps = Seq("akka-http-core", "akka-http").map(p => "com.typesafe.akka" %% p % AKKA_HTTP_VERSION)

  val zioDeps = Seq(
    "dev.zio" %% "zio" % ZIO_VERSION,
    "dev.zio" %% "zio-streams" % ZIO_VERSION,
    "dev.zio" %% "zio-logging-slf4j" % "0.2.7"
  )

  val zioTestDeps = Seq(
    "dev.zio" %% "zio-test" % ZIO_VERSION,
    "dev.zio" %% "zio-test-sbt" % ZIO_VERSION,
    "dev.zio" %% "zio-test-magnolia" % ZIO_VERSION,
    "dev.zio" %% "zio-test-junit" % ZIO_VERSION
  )

  val squantsDeps = Seq(
    "org.typelevel" %% "squants" % "1.6.0"
  )

  val prometheusClientDeps = Seq(
    "io.prometheus" % "simpleclient" % "0.8.1"
  )

  val akkaGrpcRuntimeDeps = Seq(
    "com.lightbend.akka.grpc" %% "akka-grpc-runtime" % "0.7.3"
  )

  val catsCoreDeps = Seq(
    "org.typelevel" %% "cats-core" % "2.1.1"
  )

  val kittensDeps = Seq(
    "org.typelevel" %% "kittens" % "2.1.0"
  )

  val pureconfigDeps = Seq("pureconfig", "pureconfig-akka")
    .map(p => "com.github.pureconfig" %% p % "0.12.3")

  val akkaTestDeps = Seq("akka-testkit", "akka-stream-testkit", "akka-actor-testkit-typed")
    .map(p => "com.typesafe.akka" %% p % AKKA_VERSION)

  val quicklensDeps = Seq(
    "com.softwaremill.quicklens" %% "quicklens" % "1.4.12"
  )

  val betterfilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.7.1"
  )

  val nameofDeps = Seq(
    "com.github.dwickern" %% "scala-nameof" % "1.0.3" % Provided
  )

  val loggingDeps = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.30",
    "org.slf4j" % "jul-to-slf4j" % "1.7.30"
  )

  val janinoDeps = Seq(
    "org.codehaus.janino" % "janino" % "3.1.2"
  )

  val scalatestDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.1.1",
    "org.scalatest" %% "scalatest" % "3.1.1",
    "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2",
    "org.scalacheck" %% "scalacheck" % "1.14.3",
    "org.scalamock" %% "scalamock" % "4.4.0"
  )

  val hamstersDeps = Seq(
    "io.github.scala-hamsters" %% "hamsters" % "3.1.0"
  )

  val rocksdbDeps = Seq(
    "org.rocksdb" % "rocksdbjni" % "6.7.3"
  )

  val lmdbDeps = Seq(
    "org.lmdbjava" % "lmdbjava" % "0.7.0"
  )

  val fdbDeps = Seq(
    "org.foundationdb" % "fdb-java" % "6.2.19"
  )

  val shapelessDeps = Seq(
    "com.chuusai" %% "shapeless" % "2.3.3"
  )

  val scalapbRuntimeDeps = Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf"
  )

  val enumeratumDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.5.15"
  )

  val refinedCoreDeps = Seq(
    "eu.timepit" %% "refined" % "0.9.14"
  )

  val refinedDeps = refinedCoreDeps ++ Seq(
    "eu.timepit" %% "refined-pureconfig" % "0.9.14"
  )

  val chimneyDeps = Seq(
    "io.scalaland" %% "chimney" % "0.5.1"
  )

  val snappyDeps = Seq(
    "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
  )

  val betterFilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.8.0"
  )

  val microlibsDeps = Seq(
    "com.github.japgolly.microlibs" %% "utils" % "2.2"
  )

  val silencerVersion = "1.6.0"

  val silencerDeps = Seq(
    compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full),
    "com.github.ghik" % "silencer-lib" % silencerVersion % Provided cross CrossVersion.full
  )

  val berkeleyDbDeps = Seq(
    "com.sleepycat" % "je" % "18.3.12"
  )

  val magnoliaDeps = Seq(
    "com.propensive" %% "magnolia" % "0.15.0"
  )

  val pprintDeps = Seq(
    "com.lihaoyi" %% "pprint" % "0.5.9"
  )

  val avro4sDeps = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.9"
  )
}
