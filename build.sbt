import Dependencies._

//Global / semanticdbEnabled := true
ThisBuild / organization := "dev.chopsticks"
ThisBuild / developers := List(
  Developer(
    id = "nktpro",
    name = "Jacky Nguyen",
    email = "nktpro@gmail.com",
    url = new URL("https://github.com/nktpro/")
  ),
  Developer(
    id = "pwliwanow",
    name = "Pawel Iwanow",
    email = "pwliwanow@gmail.com",
    url = new URL("https://github.com/pwliwanow/")
  )
)

ThisBuild / description := "Essential Scala libraries for everyday use"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://chopsticks.dev"))

ThisBuild / resolvers ++= Build.resolvers
ThisBuild / scalaVersion := SCALA_VERSION
ThisBuild / javacOptions ++= Build.javacOptions
ThisBuild / scalacOptions ++= Build.scalacOptions

ThisBuild / Test / testOptions := Seq(Tests.Argument("-l", Build.ItTagName), Tests.Cleanup(() => System.gc()))
//ThisBuild / Build.ITest / testOptions := Seq(Tests.Argument("-n", Build.ItTagName), Tests.Cleanup(() => System.gc()))

ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions ++= Seq(
  "-Xmx2g",
  "-XX:ActiveProcessorCount=2"
)
//ThisBuild / Build.ITest / fork := Build.forkTests
//ThisBuild / Build.ITest / javaOptions += "-Xmx1g"

ThisBuild / PB.protocVersion := "3.21.11"
ThisBuild / versionScheme := Some("semver-spec")

lazy val integrationTestSettings = inConfig(Build.ITest)(Defaults.testTasks)

lazy val schema = Build
  .defineProject("schema")
  .settings(
    libraryDependencies ++= Dependencies.schemaDeps
  )

lazy val util = Build
  .defineProject("util")
  .settings(
    libraryDependencies ++= utilDeps
//      akkaSlf4jDeps ++ squantsDeps ++ loggingDeps ++
//      pureconfigDeps ++ microlibsDeps ++ prometheusClientDeps ++ refinedDeps
  )

lazy val testkit = Build
  .defineProject("testkit")
  .settings(
    libraryDependencies ++= testkitDeps
  )
  .dependsOn(fp, util)

lazy val fp = Build
  .defineProject("fp")
  .settings(
    libraryDependencies ++= fpDeps
  )
  .dependsOn(util)

lazy val stream = Build
  .defineProject("stream")
  .settings(
    libraryDependencies ++= streamDeps
  )
  .dependsOn(fp, testkit % "test->test")

//lazy val dstream = Build
//  .defineProject("dstream")
//  .enablePlugins(AkkaGrpcPlugin)
//  .settings(
//    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
//    libraryDependencies ++= akkaGrpcRuntimeDeps ++ enumeratumDeps ++ (zioMagicDeps ++ akkaTestDeps ++ zioTestDeps).map(
//      _ % "test"
//    ),
//    akkaGrpcCodeGeneratorSettings += "server_power_apis",
//    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
//  )
//  .dependsOn(metric, stream)
//
lazy val kvdb = Build
  .defineProject("kvdb")
  .settings(
    libraryDependencies ++= kvdbDeps,
    Compile / PB.targets := Seq(
      scalapb
        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Compile / sourceManaged).value
    )
  )
  .dependsOn(util, fp, schema, stream, testkit % "test->test")

//lazy val graphql = Build
//  .defineProject("graphql")
//  .settings(
//    libraryDependencies ++= calibanDeps
//  )
//  .dependsOn(fp, stream)
//

lazy val metric = Build
  .defineProject("metric")
  .settings(
    libraryDependencies ++= metricDeps
//      prometheusClientDeps ++ pureconfigDeps ++ zioCoreDeps ++ akkaStreamDeps ++ scalatestDeps.map(
//      _ % "test"
//    )
  )
  .dependsOn(fp)

//lazy val alertmanager = Build
//  .defineProject("alertmanager")
//  .settings(
//    libraryDependencies ++= circeDeps ++ enumeratumDeps ++ enumeratumCirceDeps
//  )

//lazy val prometheus = Build
//  .defineProject("prometheus")
//  .settings(Build.createScalapbSettings(withGrpc = false))
//  .settings(
//    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
//    libraryDependencies ++= scalapbRuntimeDeps
//  )

//lazy val zioGrpcCommon = Build
//  .defineProject("zio-grpc-common")
//  .settings(Build.createScalapbSettings(withGrpc = true))
//  .settings(
//    Compile / PB.targets += scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value,
//    libraryDependencies ++= scalapbRuntimeDeps ++ scalapbRuntimeGrpcDeps
//  )
//  .dependsOn(util)

//lazy val jwt = Build
//  .defineProject("jwt")
//  .settings(
//    libraryDependencies ++= jwtCirceDeps ++ zioDeps
//  )
//  .dependsOn(util)

lazy val sample = Build
  .defineProject("sample")
  .settings(
    publish / skip := true,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(
    kvdb,
    metric,
//    dstream,
    testkit % "test->test"
  )

//lazy val avro4sShadowed = Build
//  .defineProject("avro4s-shadowed")
//  .settings(
//    libraryDependencies ++= avro4sDeps,
//    assembly / assemblyExcludedJars := {
//      val cp = (assembly / fullClasspath).value
//      cp.filterNot(_.data.getName.startsWith("avro4s-"))
//    },
//    assemblyMergeStrategy := {
//      case PathList("com", "sksamuel", "avro4s", _*) => MergeStrategy.first
//      case x =>
//        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
//        oldStrategy(x)
//    },
//    publish / skip := true
//  )

//lazy val avro4s = Build
//  .defineProject("avro4s")
//  .settings(
//    libraryDependencies ++= avro4sDirectDeps,
//    Compile / packageBin := (avro4sShadowed / assembly).value
//  )

//lazy val openapi = Build
//  .defineProject("openapi")
//  .settings(
//    libraryDependencies ++= tapirDeps ++ zioSchemaDeps
//  )
//  .dependsOn(util)

//lazy val csv = Build
//  .defineProject("csv")
//  .dependsOn(openapi)

lazy val root = (project in file("."))
  .settings(
    name := "chopsticks",
    publish / skip := true,
    dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang")
//    Build.ossPublishSettings
  )
  .aggregate(
    schema,
    util,
    testkit,
    fp,
//    dstream,
    stream,
//    graphql,
    kvdb,
    metric,
//    openapi,
//    csv,
//    alertmanager,
//    prometheus,
//    zioGrpcCommon,
    sample
//    avro4sShadowed,
//    avro4s,
//    jwt
  )
