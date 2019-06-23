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
Global / concurrentRestrictions ++= (if (!Build.forkTests) Seq(Tags.limit(Tags.Test, 2)) else Seq.empty[Tags.Rule])

ThisBuild / symlinkTargetRoot := Build.symlinkTargetRoot

ThisBuild / licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))

lazy val integrationTestSettings = inConfig(Build.ITest)(Defaults.testTasks)

lazy val util = Build
  .defineProject("util")
  .settings(
    libraryDependencies ++= akkaSlf4jDeps ++ kamonCoreDeps ++ kamonProdDeps
      ++ squantsDeps ++ loggingDeps ++ pureconfigDeps ++ microlibsDeps
  )

lazy val testkit = Build
  .defineProject("testkit")
  .settings(
    libraryDependencies ++= akkaTestDeps ++ scalatestDeps
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

lazy val kvdb = Build
  .defineProject("kvdb")
  .settings(
    libraryDependencies ++= akkaHttpDeps ++ rocksdbDeps ++ lmdbDeps ++
      shapelessDeps ++ scalapbRuntimeDeps ++ enumeratumDeps ++ chimneyDeps ++
      snappyDeps ++ pureconfigDeps ++ kamonCoreDeps ++ betterFilesDeps ++
      refinedDeps ++ silencerDeps,
    Compile / PB.targets := Seq(
      scalapb
        .gen(flatPackage = true, singleLineToProtoString = true, lenses = false) -> (Compile / sourceManaged).value
    ),
    scalacOptions ++= Seq(
      s"-P:silencer:sourceRoots=${(Compile / sourceManaged).value.getCanonicalPath}",
      "-P:silencer:pathFilters=dev/chopsticks/proto"
    )
  )
  .dependsOn(util, fp, stream, testkit % "test->compile")

lazy val root = (project in file("."))
  .enablePlugins(SymlinkTargetPlugin)
  .settings(
    name := "chopsticks",
    publish / skip := true
  )
  .aggregate(util, testkit, fp, stream, dstream, kvdb)
