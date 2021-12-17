import sbt._

//noinspection ScalaUnusedSymbol,TypeAnnotation
object Dependencies {
  val SCALA_VERSION = "2.13.7"
  val AKKA_VERSION = "2.6.17"
  val AKKA_HTTP_VERSION = "10.2.1"
  val ZIO_VERSION = "1.0.12"
  val IZUMI_VERSION = "1.0.8"
  val REFINED_VERSION = "0.9.28"
  val CALIBAN_VERSION = "1.2.4"

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

  val zioCoreDeps = Seq(
    "dev.zio" %% "zio" % ZIO_VERSION
  )

  val zioDeps = zioCoreDeps ++ Seq(
    "dev.zio" %% "zio-streams" % ZIO_VERSION
  )

  lazy val zioInteropReactivestreamsDeps = Seq(
    "dev.zio" %% "zio-interop-reactivestreams" % "1.3.8"
  )

  val zioTestDeps = Seq(
    "dev.zio" %% "zio-test" % ZIO_VERSION,
    "dev.zio" %% "zio-test-sbt" % ZIO_VERSION,
    "dev.zio" %% "zio-test-magnolia" % ZIO_VERSION,
    "dev.zio" %% "zio-test-junit" % ZIO_VERSION
  )

  val zioMagicDeps = Seq(
    "io.github.kitlangton" %% "zio-magic" % "0.3.11"
  )

  val squantsDeps = Seq(
    "org.typelevel" %% "squants" % "1.7.4"
  )

  val prometheusClientDeps = Seq(
    "io.prometheus" % "simpleclient" % "0.12.0",
    "io.prometheus" % "simpleclient_common" % "0.12.0"
  )

  val akkaGrpcRuntimeDeps = Seq(
    "com.lightbend.akka.grpc" %% "akka-grpc-runtime" % "2.1.2"
  )

  val catsCoreDeps = Seq(
    "org.typelevel" %% "cats-core" % "2.7.0"
  )

  val kittensDeps = Seq(
    "org.typelevel" %% "kittens" % "2.3.2"
  )

  val pureconfigDeps = Seq("pureconfig", "pureconfig-akka")
    .map(p => "com.github.pureconfig" %% p % "0.17.1")

  val akkaTestDeps = Seq("akka-testkit", "akka-stream-testkit", "akka-actor-testkit-typed")
    .map(p => "com.typesafe.akka" %% p % AKKA_VERSION)

  val nameofDeps = Seq(
    "com.github.dwickern" %% "scala-nameof" % "3.0.0" % Provided
  )

  val loggingDeps = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    "io.7mind.izumi" %% "logstage-adapter-slf4j" % IZUMI_VERSION,
    "org.slf4j" % "jul-to-slf4j" % "1.7.32"
  )

  val janinoDeps = Seq(
    "org.codehaus.janino" % "janino" % "3.1.6"
  )

  val scalatestDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.2.10",
    "org.scalatest" %% "scalatest" % "3.2.10",
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0",
    "org.scalacheck" %% "scalacheck" % "1.15.4",
    "org.scalamock" %% "scalamock" % "5.1.0"
  )

  val hamstersDeps = Seq(
    "io.github.scala-hamsters" %% "hamsters" % "3.1.0"
  )

  val rocksdbDeps = Seq(
    "org.rocksdb" % "rocksdbjni" % "6.26.1"
  )

  val lmdbDeps = Seq(
    "org.lmdbjava" % "lmdbjava" % "0.8.2"
  )

  val fdbDeps = Seq(
    "org.foundationdb" % "fdb-java" % "6.3.22"
  )

  val shapelessDeps = Seq(
    "com.chuusai" %% "shapeless" % "2.3.7"
  )

  val scalapbRuntimeDeps = Seq(
    ("com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion)
      .exclude("io.grpc", "grpc-stub")
      .exclude("io.grpc", "grpc-protobuf"),
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

  val scalapbRuntimeGrpcDeps = Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion
  )

  val enumeratumDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.7.0"
  )

  val refinedDeps = Seq(
    "eu.timepit" %% "refined" % REFINED_VERSION,
    "eu.timepit" %% "refined-shapeless" % REFINED_VERSION
  )

  val chimneyDeps = Seq(
    "io.scalaland" %% "chimney" % "0.6.1"
  )

  val snappyDeps = Seq(
    "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
  )

  val betterFilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.9.1"
  )

  val microlibsDeps = Seq(
    "com.github.japgolly.microlibs" %% "utils" % "4.0.0"
  )

  val berkeleyDbDeps = Seq(
    "com.sleepycat" % "je" % "18.3.12"
  )

  val magnoliaDeps = Seq(
    "com.propensive" %% "magnolia" % "0.17.0"
  )

  val pprintDeps = Seq(
    "com.lihaoyi" %% "pprint" % "0.6.6"
  )

  val distageDeps = Seq(
    "io.7mind.izumi" %% "distage-core",
    "io.7mind.izumi" %% "logstage-core",
    "io.7mind.izumi" %% "logstage-rendering-circe",
    "io.7mind.izumi" %% "distage-extension-logstage",
    "io.7mind.izumi" %% "logstage-sink-slf4j"
  ).map(_ % IZUMI_VERSION)

  val calibanDeps =
    Seq(
      "com.github.ghostdogpr" %% "caliban",
      "com.github.ghostdogpr" %% "caliban-client",
      "com.github.ghostdogpr" %% "caliban-akka-http"
    )
      .map(_ % CALIBAN_VERSION)

  val sourcecodeDeps = Seq(
    "com.lihaoyi" %% "sourcecode" % "0.2.7"
  )
}
