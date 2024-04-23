import sbt._

//noinspection ScalaUnusedSymbol,TypeAnnotation
object Dependencies {
  val SCALA_VERSION = "2.13.10"
  val PEKKO_VERSION = "1.0.2"
  val PEKKO_HTTP_VERSION = "1.0.1"
  val ZIO_VERSION = "2.0.21"
  val IZUMI_VERSION = "1.2.6"
  val REFINED_VERSION = "0.9.29"
  val CIRCE_VERSION = "0.14.3"
  val CALIBAN_VERSION = "2.5.3"

  val pekkoSlf4jDeps = Seq(
    "org.apache.pekko" %% "pekko-slf4j" % PEKKO_VERSION
  )

  val pekkoStreamDeps = Seq(
    "pekko-stream",
    "pekko-stream-typed"
  ).map { p => "org.apache.pekko" %% p % PEKKO_VERSION }

  val pekkoActorDeps = Seq(
    "pekko-actor",
    "pekko-actor-typed"
  ).map { p => "org.apache.pekko" %% p % PEKKO_VERSION }

  val pekkoDiscoveryOverrideDeps = Seq(
    "pekko-discovery"
  ).map { p => "org.apache.pekko" %% p % PEKKO_VERSION }

  val pekkoHttpDeps = Seq("pekko-http-core", "pekko-http").map(p => "org.apache.pekko" %% p % PEKKO_HTTP_VERSION)

  val zioCoreDeps = Seq(
    // Override izumi-reflect to 2.3.7 which solves the dreaded "Defect in zio.Has" bug again
    "dev.zio" %% "izumi-reflect" % "2.3.8",
    "dev.zio" %% "zio" % ZIO_VERSION
  )

  val zioDeps = zioCoreDeps ++ Seq(
    "dev.zio" %% "zio-streams" % ZIO_VERSION
  )

  lazy val zioInteropReactivestreamsDeps = Seq(
    "dev.zio" %% "zio-interop-reactivestreams" % "2.0.2"
  )

  val zioTestDeps = Seq(
    "dev.zio" %% "zio-test" % ZIO_VERSION,
    "dev.zio" %% "zio-test-sbt" % ZIO_VERSION,
    "dev.zio" %% "zio-test-magnolia" % ZIO_VERSION,
    "dev.zio" %% "zio-test-junit" % ZIO_VERSION
  )

  val zioMagicDeps = Seq(
    "io.github.kitlangton" %% "zio-magic" % "0.3.12"
  )

  val squantsDeps = Seq(
    "org.typelevel" %% "squants" % "1.7.4"
  )

  val prometheusClientDeps = Seq(
    "io.prometheus" % "simpleclient" % "0.16.0",
    "io.prometheus" % "simpleclient_common" % "0.16.0"
  )

  val pekkoGrpcRuntimeDeps = Seq(
    "org.apache.pekko" %% "pekko-grpc-runtime" % "1.0.0"
  )

  val catsCoreDeps = Seq(
    "org.typelevel" %% "cats-core" % "2.8.0"
  )

  val pureconfigDeps = Seq("pureconfig", "pureconfig-pekko")
    .map(p => "com.github.pureconfig" %% p % "0.17.6")

  val pekkoTestDeps = Seq("pekko-testkit", "pekko-stream-testkit", "pekko-actor-testkit-typed")
    .map(p => "org.apache.pekko" %% p % PEKKO_VERSION)

  val nameofDeps = Seq(
    "com.github.dwickern" %% "scala-nameof" % "4.0.0" % Provided
  )

  val loggingDeps = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "io.7mind.izumi" %% "logstage-adapter-slf4j" % IZUMI_VERSION,
    "org.slf4j" % "jul-to-slf4j" % "1.7.36"
  )

  val janinoDeps = Seq(
    "org.codehaus.janino" % "janino" % "3.1.9"
  )

  val scalatestDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.2.10",
    "org.scalatest" %% "scalatest" % "3.2.10",
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0",
    "org.scalacheck" %% "scalacheck" % "1.15.4",
    "org.scalamock" %% "scalamock" % "5.2.0"
  )

  val hamstersDeps = Seq(
    "io.github.scala-hamsters" %% "hamsters" % "3.1.0"
  )

  val rocksdbDeps = Seq(
    "org.rocksdb" % "rocksdbjni" % "7.10.2"
  )

  val lmdbDeps = Seq(
    "org.lmdbjava" % "lmdbjava" % "0.8.3"
  )

  val fdbDeps = Seq(
    "org.foundationdb" % "fdb-java" % "7.1.53"
  )

  val shapelessDeps = Seq(
    "com.chuusai" %% "shapeless" % "2.3.9"
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

  val enumeratumCirceDeps = Seq(
    "com.beachape" %% "enumeratum-circe" % "1.7.0"
  )

  val circeDeps = Seq(
    "io.circe" %% "circe-core" % CIRCE_VERSION,
    "io.circe" %% "circe-parser" % CIRCE_VERSION,
    "io.circe" %% "circe-generic" % CIRCE_VERSION
  )

  val refinedDeps = Seq(
    "eu.timepit" %% "refined" % REFINED_VERSION,
    "eu.timepit" %% "refined-shapeless" % REFINED_VERSION
  )

  val chimneyDeps = Seq(
    "io.scalaland" %% "chimney" % "0.7.1"
  )

  val snappyDeps = Seq(
    "org.xerial.snappy" % "snappy-java" % "1.1.9.1"
  )

  val betterFilesDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.9.2"
  )

  val microlibsDeps = Seq(
    "com.github.japgolly.microlibs" %% "utils" % "4.2.1"
  )

  val berkeleyDbDeps = Seq(
    "com.sleepycat" % "je" % "18.3.12"
  )

  val pekkoConnectorUdpDeps = Seq(
    "org.apache.pekko" %% "pekko-connectors-udp" % "1.0.2"
  )

  val jsoniterDeps = Seq(
    "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.28.4",
    "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.28.4" % Provided
  )

  val magnoliaDeps = Seq(
    "com.propensive" %% "magnolia" % "0.17.0"
  )

  val pprintDeps = Seq(
    "com.lihaoyi" %% "pprint" % "0.8.1"
  )

  val logstageDeps = Seq(
    "io.7mind.izumi" %% "logstage-core",
    "io.7mind.izumi" %% "logstage-rendering-circe",
    "io.7mind.izumi" %% "logstage-sink-slf4j"
  ).map(_ % IZUMI_VERSION)

  val calibanDeps =
    Seq(
      "com.github.ghostdogpr" %% "caliban",
      "com.github.ghostdogpr" %% "caliban-client",
      "com.github.ghostdogpr" %% "caliban-pekko-http"
    )
      .map(c => (c % CALIBAN_VERSION).exclude("org.apache.pekko", "pekko-serialization-jackson_2.13"))

  val sourcecodeDeps = Seq(
    "com.lihaoyi" %% "sourcecode" % "0.3.0"
  )

  val avro4sDeps = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.0",
    "com.sksamuel.avro4s" %% "avro4s-refined" % "4.1.0"
  )

  val avro4sDirectDeps = {
    val Json4sVersion = "4.0.5"
    val RefinedVersion = "0.9.29"
    val ShapelessVersion = "2.3.9"
    val MagnoliaVersion = "0.17.0"

    // https://github.com/sksamuel/avro4s/blob/release/4.0.x/build.sbt
    Seq(
      "com.propensive" %% "magnolia" % MagnoliaVersion,
      "com.chuusai" %% "shapeless" % ShapelessVersion,
      "org.json4s" %% "json4s-native" % Json4sVersion,
      "eu.timepit" %% "refined" % RefinedVersion
    )
  }

  val commonsText = Seq("org.apache.commons" % "commons-text" % "1.11.0")

  lazy val tapirDeps = {
    val tapirVersion = "1.9.10"
    Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-cats" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-enumeratum" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapirVersion,
//      "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-refined" % tapirVersion
    )
  }

  lazy val zioSchemaDeps = {
    val zioSchemaOrganization = "dev.zio"
    val zioSchemaVersion = "1.1.1"
    Seq(
      zioSchemaOrganization %% "zio-schema" % zioSchemaVersion,
      zioSchemaOrganization %% "zio-schema-json" % zioSchemaVersion,
      zioSchemaOrganization %% "zio-schema-derivation" % zioSchemaVersion
    )
  }

  val jwtCirceDeps = Seq(
    "com.github.jwt-scala" %% "jwt-circe" % "9.1.1"
  )
}
