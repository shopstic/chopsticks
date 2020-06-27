import Dependencies._

ThisBuild / organization := "dev.chopsticks"
ThisBuild / scalaVersion := SCALA_VERSION
ThisBuild / javacOptions ++= Build.javacOptions
ThisBuild / scalacOptions ++= Build.scalacOptions

ThisBuild / Test / testOptions := Seq(Tests.Argument("-l", Build.ItTagName), Tests.Cleanup(() => System.gc()))
ThisBuild / Build.ITest / testOptions := Seq(Tests.Argument("-n", Build.ItTagName), Tests.Cleanup(() => System.gc()))

ThisBuild / Test / fork := Build.forkTests
ThisBuild / Test / javaOptions += "-Xmx768m"
ThisBuild / Build.ITest / fork := Build.forkTests
ThisBuild / Build.ITest / javaOptions += "-Xmx1g"

ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))
ThisBuild / bintrayReleaseOnPublish := false
ThisBuild / resolvers ++= Seq(
  Resolver.bintrayRepo("shopstic", "maven"),
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

lazy val integrationTestSettings = inConfig(Build.ITest)(Defaults.testTasks)

lazy val util = Build
  .defineProject("util")
  .settings(
    libraryDependencies ++= akkaSlf4jDeps ++ squantsDeps ++ loggingDeps ++
      pureconfigDeps ++ microlibsDeps ++ prometheusClientDeps
  )

lazy val testkit = Build
  .defineProject("testkit")
  .settings(
    libraryDependencies ++= akkaTestDeps ++ scalatestDeps ++ janinoDeps ++ zioTestDeps,
    Compile / packageBin / mappings ~= { _.filter(_._1.name != "logback-test.xml") }
  )
  .dependsOn(util)

lazy val fp = Build
  .defineProject("fp")
  .settings(
    libraryDependencies ++= akkaStreamDeps ++ zioDeps ++ distageDeps
  )
  .dependsOn(util, testkit % "test->test")

lazy val stream = Build
  .defineProject("stream")
  .settings(
    libraryDependencies ++= catsCoreDeps /* ++ hamstersDeps.map(_ % Test)*/
  )
  .dependsOn(fp, testkit % "test->test")

//lazy val dstream = Build
//  .defineProject("dstream")
//  .settings(
//    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
//    libraryDependencies ++= akkaGrpcRuntimeDeps
//  )
//  .dependsOn(stream)

lazy val kvdbCore = Build
  .defineProject("kvdb-core")
  .settings(
    libraryDependencies ++= shapelessDeps ++ scalapbRuntimeDeps ++ chimneyDeps ++
      kittensDeps ++ betterFilesDeps ++ refinedDeps,
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

lazy val kvdbRemote = Build
  .defineProject("kvdb-remote")
  .settings(
    libraryDependencies ++= akkaHttpDeps ++ snappyDeps
  )
  .dependsOn(kvdbCore)

lazy val kvdbFdb = Build
  .defineProject("kvdb-fdb")
  .settings(
    libraryDependencies ++= fdbDeps
  )
  .dependsOn(kvdbCore % "compile->compile;test->test", testkit % "test->test")

//lazy val avro4s = Build
//  .defineProject("avro4s")
//  .settings(
//    libraryDependencies ++= avro4sDeps ++ refinedCoreDeps
//  )
//  .dependsOn(util)

lazy val kvdbCodecFdbKey = Build
  .defineProject("kvdb-codec-fdb-key")
  .settings(
    libraryDependencies ++= fdbDeps ++ magnoliaDeps ++ enumeratumDeps ++ refinedCoreDeps
  )
  .dependsOn(kvdbCore, testkit % "test->test")

lazy val kvdbCodecBerkeleydbKey = Build
  .defineProject("kvdb-codec-berkeleydb-key")
  .settings(
    libraryDependencies ++= berkeleyDbDeps ++ magnoliaDeps ++ enumeratumDeps ++ refinedCoreDeps
  )
  .dependsOn(kvdbCore, testkit % "test->test")

lazy val kvdbCodecProtobufValue = Build
  .defineProject("kvdb-codec-protobuf-value")
  .dependsOn(kvdbCore)

lazy val metric = Build
  .defineProject("metric")
  .settings(
    libraryDependencies ++= prometheusClientDeps ++ pureconfigDeps ++ zioCoreDeps ++ akkaStreamDeps
  )

lazy val sample = Build
  .defineProject("sample")
  .enablePlugins(AkkaGrpcPlugin)
  .settings(
    dependencyOverrides ++= akkaDiscoveryOverrideDeps,
    libraryDependencies ++= janinoDeps ++ pprintDeps,
    publish / skip := true,
    bintrayRelease := {},
    akkaGrpcCodeGeneratorSettings += "server_power_apis",
    scalacOptions ++= Seq(
      s"-Wconf:src=${(Compile / sourceManaged).value.getCanonicalPath}/dev/chopsticks/sample/app/proto/.*&cat=deprecation:s"
    )
  )
  .dependsOn(
    kvdbLmdb,
    kvdbRocksdb,
    kvdbCodecBerkeleydbKey,
    kvdbCodecProtobufValue,
    kvdbCodecFdbKey,
    metric,
    kvdbFdb /*, dstream*/
  )

lazy val root = (project in file("."))
  .settings(
    name := "chopsticks",
    publish / skip := true,
    bintrayRelease := {},
    dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang")
  )
  .aggregate(
    util,
    testkit,
    fp,
    stream,
//    dstream,
    kvdbCore,
    kvdbLmdb,
    kvdbRocksdb,
    kvdbFdb,
    kvdbCodecBerkeleydbKey,
    kvdbCodecFdbKey,
    kvdbCodecProtobufValue,
    metric,
    sample
  )
