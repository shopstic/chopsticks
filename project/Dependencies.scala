import sbt._
import scalapb.compiler.Version.scalapbVersion

//noinspection ScalaUnusedSymbol,TypeAnnotation
object Dependencies {
  val SCALA_VERSION = "2.13.5"
  val AKKA_VERSION = "2.6.10"
  val AKKA_HTTP_VERSION = "10.2.1"
  val ZIO_VERSION = "1.0.5"
  val IZUMI_VERSION = "1.0.4"
  val REFINED_VERSION = "0.9.21"
  val CALIBAN_VERSION = "0.9.5"

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
  ).overrideIzumiReflect

  val zioDeps = zioCoreDeps ++ Seq(
    "dev.zio" %% "zio-streams" % ZIO_VERSION
  ).overrideIzumiReflect

  val zioTestDeps = Seq(
    "dev.zio" %% "zio-test" % ZIO_VERSION,
    "dev.zio" %% "zio-test-sbt" % ZIO_VERSION,
    "dev.zio" %% "zio-test-magnolia" % ZIO_VERSION,
    "dev.zio" %% "zio-test-junit" % ZIO_VERSION
  ).overrideIzumiReflect

  val zioMagicDeps = Seq(
    "io.github.kitlangton" %% "zio-magic" % "0.2.2"
  )

  val squantsDeps = Seq(
    "org.typelevel" %% "squants" % "1.7.4"
  )

  val prometheusClientDeps = Seq(
    "io.prometheus" % "simpleclient" % "0.10.0",
    "io.prometheus" % "simpleclient_common" % "0.10.0"
  )

  val akkaGrpcRuntimeDeps = Seq(
    "com.lightbend.akka.grpc" %% "akka-grpc-runtime" % "1.1.1"
  )

  val catsCoreDeps = Seq(
    "org.typelevel" %% "cats-core" % "2.4.2"
  )

  val kittensDeps = Seq(
    "org.typelevel" %% "kittens" % "2.2.1"
  )

  val pureconfigDeps = Seq("pureconfig", "pureconfig-akka")
    .map(p => "com.github.pureconfig" %% p % "0.14.1")
    .excludeAkkaActor

  val akkaTestDeps = Seq("akka-testkit", "akka-stream-testkit", "akka-actor-testkit-typed")
    .map(p => "com.typesafe.akka" %% p % AKKA_VERSION)

  val nameofDeps = Seq(
    "com.github.dwickern" %% "scala-nameof" % "1.0.3" % Provided
  )

  val loggingDeps = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    "io.7mind.izumi" %% "logstage-adapter-slf4j" % IZUMI_VERSION,
    "org.slf4j" % "jul-to-slf4j" % "1.7.30"
  )

  val janinoDeps = Seq(
    "org.codehaus.janino" % "janino" % "3.1.3"
  )

  val scalatestDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.2.6",
    "org.scalatest" %% "scalatest" % "3.2.6",
    "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2",
    "org.scalacheck" %% "scalacheck" % "1.15.3",
    "org.scalamock" %% "scalamock" % "5.1.0"
  )

  val hamstersDeps = Seq(
    "io.github.scala-hamsters" %% "hamsters" % "3.1.0"
  )

  val rocksdbDeps = Seq(
    "org.rocksdb" % "rocksdbjni" % "6.15.5"
  )

  val lmdbDeps = Seq(
    "org.lmdbjava" % "lmdbjava" % "0.8.1"
  )

  val fdbDeps = Seq(
    "org.foundationdb" % "fdb-java" % "6.2.22" // scala-steward:off
  )

  val shapelessDeps = Seq(
    "com.chuusai" %% "shapeless" % "2.3.3"
  )

  val scalapbRuntimeDeps = Seq(
    ("com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion)
      .exclude("io.grpc", "grpc-stub")
      .exclude("io.grpc", "grpc-protobuf"),
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  )

  val scalapbRuntimeGrpcDeps = Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "io.grpc" % "grpc-netty" % "1.36.0"
  )

  val enumeratumDeps = Seq(
    "com.beachape" %% "enumeratum" % "1.6.1"
  )

  val refinedDeps = Seq(
    "eu.timepit" %% "refined" % REFINED_VERSION,
    "eu.timepit" %% "refined-shapeless" % REFINED_VERSION
  )

  val chimneyDeps = Seq(
    "io.scalaland" %% "chimney" % "0.6.1"
  )

  val snappyDeps = Seq(
    "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
  )

  val betterFilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.9.1"
  )

  val microlibsDeps = Seq(
    "com.github.japgolly.microlibs" %% "utils" % "2.5"
  )

  val berkeleyDbDeps = Seq(
    "com.sleepycat" % "je" % "18.3.12"
  )

  val magnoliaDeps = Seq(
    "com.propensive" %% "magnolia" % "0.17.0"
  )

  val pprintDeps = Seq(
    "com.lihaoyi" %% "pprint" % "0.6.2"
  )

  val distageDeps = Seq(
    "io.7mind.izumi" %% "distage-core" % IZUMI_VERSION,
    "io.7mind.izumi" %% "logstage-core" % IZUMI_VERSION,
    "io.7mind.izumi" %% "logstage-rendering-circe" % IZUMI_VERSION,
    "io.7mind.izumi" %% "distage-extension-logstage" % IZUMI_VERSION,
    "io.7mind.izumi" %% "logstage-sink-slf4j" % IZUMI_VERSION
  ).overrideIzumiReflect

  val sttpBackendDeps = Seq("com.softwaremill.sttp.client" %% "akka-http-backend" % "2.2.9")

  val calibanDeps =
    Seq(
      "com.github.ghostdogpr" %% "caliban",
      "com.github.ghostdogpr" %% "caliban-client",
      "com.github.ghostdogpr" %% "caliban-akka-http"
    )
      .map(_ % CALIBAN_VERSION)
      .overrideAkkaSerializationJackson
      .overrideAkkaStreams
      .overrideIzumiReflect

  val sourcecodeDeps = Seq(
    "com.lihaoyi" %% "sourcecode" % "0.2.4"
  )

  implicit private class ModulesOps(modules: Seq[ModuleID]) {
    // TODO Remove this once this issue is resolved https://github.com/zio/izumi-reflect/issues/98
    def overrideIzumiReflect: Seq[ModuleID] =
      modules.map(_.exclude("dev.zio", "izumi-reflect_2.13")) ++
        Seq("dev.zio" %% "izumi-reflect" % "1.0.0-M7")

    def overrideAkkaSerializationJackson: Seq[ModuleID] =
      modules.map(_.exclude("com.typesafe.akka", "akka-serialization-jackson_2.13")) ++
        Seq("com.typesafe.akka" %% "akka-serialization-jackson" % AKKA_VERSION)

    // todo remove this once this issue is resolved https://github.com/akka/akka/issues/30071
    def overrideAkkaStreams: Seq[ModuleID] =
      modules.map(_.exclude("com.typesafe.akka", "akka-stream_2.13")) ++ akkaStreamDeps

    // todo remove this once this issue is resolved https://github.com/akka/akka/issues/30071
    def excludeAkkaActor: Seq[ModuleID] =
      modules.map(_.exclude("com.typesafe.akka", "akka-actor_2.13"))
  }
}
