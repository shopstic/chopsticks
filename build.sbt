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

ThisBuild / scalaVersion := SCALA_VERSION
ThisBuild / javacOptions ++= Build.javacOptions
ThisBuild / scalacOptions ++= Build.scalacOptions

ThisBuild / Test / testOptions := Seq(Tests.Argument("-l", Build.ItTagName), Tests.Cleanup(() => System.gc()))
//ThisBuild / Build.ITest / testOptions := Seq(Tests.Argument("-n", Build.ItTagName), Tests.Cleanup(() => System.gc()))

//ThisBuild / Test / fork := false
//ThisBuild / Test / javaOptions += "-Xmx2g"
//ThisBuild / Build.ITest / fork := Build.forkTests
//ThisBuild / Build.ITest / javaOptions += "-Xmx1g"

ThisBuild / PB.protocVersion := "3.17.3"
ThisBuild / versionScheme := Some("semver-spec")

lazy val integrationTestSettings = inConfig(Build.ITest)(Defaults.testTasks)

lazy val util = Build
  .defineProject("util")
  .settings(
    libraryDependencies ++= akkaSlf4jDeps ++ squantsDeps ++ loggingDeps ++
      pureconfigDeps ++ microlibsDeps ++ prometheusClientDeps ++ refinedDeps
  )

lazy val testkit = Build
  .defineProject("testkit")
  .settings(
    libraryDependencies ++= akkaTestDeps ++ scalatestDeps ++ janinoDeps ++ zioTestDeps,
    Compile / packageBin / mappings ~= { _.filter(_._1.name != "logback-test.xml") }
  )
  .dependsOn(fp, util)

lazy val fp = Build
  .defineProject("fp")
  .settings(
    libraryDependencies ++= akkaStreamDeps ++ zioDeps ++ distageDeps ++ sourcecodeDeps ++ zioMagicDeps.map(_ % "test")
  )
  .dependsOn(util)

lazy val stream = Build
  .defineProject("stream")
  .settings(
    libraryDependencies ++= catsCoreDeps /* ++ hamstersDeps.map(_ % Test)*/
  )
  .dependsOn(fp, testkit % "test->test")

lazy val dstream = Build
  .defineProject("dstream")
  .enablePlugins(AkkaGrpcPlugin)
  .settings(
    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
    libraryDependencies ++= akkaGrpcRuntimeDeps ++ enumeratumDeps ++ (zioMagicDeps ++ akkaTestDeps ++ zioTestDeps).map(
      _ % "test"
    ),
    akkaGrpcCodeGeneratorSettings += "server_power_apis",
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(metric, stream)

lazy val kvdbCore = Build
  .defineProject("kvdb-core")
  .settings(
    libraryDependencies ++= shapelessDeps ++ scalapbRuntimeDeps ++ chimneyDeps ++
      kittensDeps ++ betterFilesDeps,
    Compile / PB.targets := Seq(
      scalapb
        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Compile / sourceManaged).value
    )
//    scalacOptions ++= Seq(
//      s"-P:silencer:sourceRoots=${(Compile / sourceManaged).value.getCanonicalPath}",
//      "-P:silencer:pathFilters=dev/chopsticks/kvdb/proto"
//    )
  )
  .dependsOn(util, fp, stream, testkit % "test->test")

lazy val kvdbLmdb = Build
  .defineProject("kvdb-lmdb")
  .settings(
    libraryDependencies ++= lmdbDeps
  )
  .dependsOn(kvdbCore % "compile->compile;test->test", testkit % "test->test")

lazy val kvdbRocksdb = Build
  .defineProject("kvdb-rocksdb")
  .settings(
    libraryDependencies ++= rocksdbDeps
  )
  .dependsOn(kvdbCore % "compile->compile;test->test", testkit % "test->test")

lazy val kvdbFdb = Build
  .defineProject("kvdb-fdb")
  .settings(
    libraryDependencies ++= fdbDeps ++ zioInteropReactivestreamsDeps
  )
  .dependsOn(
    kvdbCore % "compile->compile;test->test",
    kvdbCodecFdbKey % "compile->compile;test->test",
    testkit % "test->test"
  )

lazy val graphql = Build
  .defineProject("graphql")
  .settings(
    libraryDependencies ++= calibanDeps
  )
  .dependsOn(fp, stream)

lazy val kvdbCodecFdbKey = Build
  .defineProject("kvdb-codec-fdb-key")
  .settings(
    libraryDependencies ++= fdbDeps ++ magnoliaDeps ++ enumeratumDeps
  )
  .dependsOn(kvdbCore, testkit % "test->test")

lazy val kvdbCodecBerkeleydbKey = Build
  .defineProject("kvdb-codec-berkeleydb-key")
  .settings(
    libraryDependencies ++= berkeleyDbDeps ++ magnoliaDeps ++ enumeratumDeps
  )
  .dependsOn(kvdbCore, testkit % "test->test")

lazy val kvdbCodecProtobufValue = Build
  .defineProject("kvdb-codec-protobuf-value")
  .settings(
    Test / PB.targets := Seq(
      scalapb
        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Test / sourceManaged).value
    ),
    Test / scalacOptions ++= Seq(
      s"-Wconf:src=${(Test / sourceManaged).value.getCanonicalPath}/.*&cat=unused-imports:s"
    )
  )
  .dependsOn(kvdbCore)

lazy val metric = Build
  .defineProject("metric")
  .settings(
    libraryDependencies ++= prometheusClientDeps ++ pureconfigDeps ++ zioCoreDeps ++ akkaStreamDeps ++ scalatestDeps.map(
      _ % "test"
    )
  )
  .dependsOn(fp)

lazy val zioGrpcCommon = Build
  .defineProject("zio-grpc-common")
  .settings(Build.createScalapbSettings(withGrpc = true))
  .settings(
    Compile / PB.targets += scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value,
    libraryDependencies ++= scalapbRuntimeDeps ++ scalapbRuntimeGrpcDeps
  )
  .dependsOn(util)

lazy val sample = Build
  .defineProject("sample")
  .enablePlugins(AkkaGrpcPlugin)
  .settings(
    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
    libraryDependencies ++= janinoDeps ++ pprintDeps ++ zioMagicDeps,
    publish / skip := true,
    akkaGrpcCodeGeneratorSettings += "server_power_apis",
    scalacOptions ++= Seq(
      s"-Wconf:src=${(Compile / sourceManaged).value.getCanonicalPath}/dev/chopsticks/sample/app/proto/.*&cat=deprecation:s"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
  .dependsOn(
    kvdbLmdb,
    kvdbRocksdb,
    kvdbCodecBerkeleydbKey,
    kvdbCodecProtobufValue,
    kvdbCodecFdbKey,
    metric,
    kvdbFdb,
    dstream,
    testkit % "test->test"
  )

lazy val root = (project in file("."))
  .settings(
    name := "chopsticks",
    publish / skip := true,
    dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang")
//    Build.ossPublishSettings
  )
  .aggregate(
    util,
    testkit,
    fp,
    dstream,
    stream,
    graphql,
    kvdbCore,
    kvdbLmdb,
    kvdbRocksdb,
    kvdbFdb,
    kvdbCodecBerkeleydbKey,
    kvdbCodecFdbKey,
    kvdbCodecProtobufValue,
    metric,
    zioGrpcCommon,
    sample
  )
