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
//Global / concurrentRestrictions ++= (if (!Build.forkTests) Seq(Tags.limit(Tags.Test, 2)) else Seq.empty[Tags.Rule])

ThisBuild / symlinkTargetRoot := Build.symlinkTargetRoot

ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))
ThisBuild / bintrayReleaseOnPublish := false

lazy val integrationTestSettings = inConfig(Build.ITest)(Defaults.testTasks)

lazy val util = Build
  .defineProject("util")
  .settings(
    libraryDependencies ++= akkaSlf4jDeps ++ kamonCoreDeps
      ++ squantsDeps ++ loggingDeps ++ pureconfigDeps ++ microlibsDeps
  )

lazy val testkit = Build
  .defineProject("testkit")
  .settings(
    libraryDependencies ++= akkaTestDeps ++ scalatestDeps ++ janinoDeps
  )
  .dependsOn(util)

lazy val fp = Build
  .defineProject("fp")
  .settings(
    libraryDependencies ++= akkaStreamDeps ++ zioDeps
  )
  .dependsOn(util)

lazy val stream = Build
  .defineProject("stream")
  .settings(
    libraryDependencies ++= catsCoreDeps ++ hamstersDeps.map(_ % Test)
  )
  .dependsOn(fp, testkit % "test->compile")

lazy val dstream = Build
  .defineProject("dstream")
  .settings(
    libraryDependencies ++= akkaGrpcRuntimeDeps
  )
  .dependsOn(fp)

lazy val kvdbCore = Build
  .defineProject("kvdb-core")
  .settings(
    libraryDependencies ++= shapelessDeps ++ scalapbRuntimeDeps ++ chimneyDeps ++
      kittensDeps ++ betterFilesDeps ++ refinedDeps ++ silencerDeps,
    Compile / PB.targets := Seq(
      scalapb
        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Compile / sourceManaged).value
    ),
    scalacOptions ++= Seq(
      s"-P:silencer:sourceRoots=${(Compile / sourceManaged).value.getCanonicalPath}",
      "-P:silencer:pathFilters=dev/chopsticks/kvdb/proto"
    )
  )
  .dependsOn(util, fp, stream, testkit % "test->compile")

lazy val kvdbLmdb = Build
  .defineProject("kvdb-lmdb")
  .settings(
    libraryDependencies ++= lmdbDeps
  )
  .dependsOn(kvdbCore % "compile->compile;test->test", testkit % "test->compile")

lazy val kvdbRocksdb = Build
  .defineProject("kvdb-rocksdb")
  .settings(
    libraryDependencies ++= rocksdbDeps
  )
  .dependsOn(kvdbCore % "compile->compile;test->test", testkit % "test->compile")

lazy val kvdbRemote = Build
  .defineProject("kvdb-remote")
  .settings(
    libraryDependencies ++= akkaHttpDeps ++ snappyDeps
  )
  .dependsOn(kvdbCore)

//lazy val kvdb = Build
//  .defineProject("kvdb")
//  .settings(
//    libraryDependencies ++= akkaHttpDeps ++ rocksdbDeps ++ lmdbDeps ++
//      shapelessDeps ++ scalapbRuntimeDeps ++ enumeratumDeps ++ chimneyDeps ++
//      kittensDeps ++ snappyDeps ++ betterFilesDeps ++ refinedDeps ++ silencerDeps,
//    Compile / PB.targets := Seq(
//      scalapb
//        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Compile / sourceManaged).value
//    ),
//    scalacOptions ++= Seq(
//      s"-P:silencer:sourceRoots=${(Compile / sourceManaged).value.getCanonicalPath}",
//      "-P:silencer:pathFilters=dev/chopsticks/kvdb/proto"
//    )
//  )
//  .dependsOn(util, fp, stream)

lazy val kvdbCodecBerkeleydbKey = Build
  .defineProject("kvdb-codec-berkeleydb-key")
  .settings(
    libraryDependencies ++= berkeleyDbDeps
  )
  .dependsOn(kvdbCore)

lazy val kvdbCodecProtobufValue = Build
  .defineProject("kvdb-codec-protobuf-value")
  .dependsOn(kvdbCore)

//lazy val kvdbTest = Build
//  .defineProject("kvdb-test")
//  .settings(
//    publish / skip := true,
//    bintrayRelease := {},
//  )
//  .dependsOn(kvdbCodecBerkeleydbKey, kvdbCodecProtobufValue, kvdbCodecPrimitive, testkit % "test->compile")

lazy val sample = Build
  .defineProject("sample")
  .enablePlugins(AkkaGrpcPlugin)
  .settings(
    libraryDependencies ++= janinoDeps ++ silencerDeps ++ pprintDeps,
    publish / skip := true,
    bintrayRelease := {},
    akkaGrpcCodeGeneratorSettings += "server_power_apis",
    scalacOptions ++= Seq(
      s"-P:silencer:sourceRoots=${(Compile / sourceManaged).value.getCanonicalPath}",
      "-P:silencer:pathFilters=dev/chopsticks/sample/app/proto"
    )
  )
  .dependsOn(kvdbCore, kvdbCodecBerkeleydbKey, kvdbCodecProtobufValue, dstream)

lazy val root = (project in file("."))
  .enablePlugins(SymlinkTargetPlugin)
  .settings(
    name := "chopsticks",
    publish / skip := true,
    bintrayRelease := {},
    dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang")
  )
  .aggregate(util, testkit, fp, stream, dstream, kvdbCore, kvdbLmdb, kvdbRocksdb/*, sample*/, kvdbCodecBerkeleydbKey, kvdbCodecProtobufValue)
