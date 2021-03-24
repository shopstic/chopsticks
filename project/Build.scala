import com.timushev.sbt.updates.UpdatesPlugin.autoImport._
import com.typesafe.sbt.GitPlugin.autoImport.git
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import protocbridge.Target
import wartremover.WartRemover.autoImport._
import sbt.{Def, _}
import sbt.Keys._
import sbtprotoc.ProtocPlugin.autoImport.PB

//noinspection TypeAnnotation
object Build {

  lazy val ITest = config("it") extend Test

  val ItTagName = "dev.chopsticks.test.tags.IntegrationTest"

  val forkTests = sys.env.get("FORK_TESTS").forall(_ == "true")

  val javacOptions = Seq("-encoding", "UTF-8")
  val scalacOptions: Seq[String] = Seq(
    "-encoding",
    "utf-8",
    "-explaintypes", // Explain type errors in more detail.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Wdead-code", // Warn when dead code is identified.
    "-Wextra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Wnumeric-widen", // Warn when numerics are widened.
    "-Wvalue-discard", // Warn when non-Unit expression results are unused.
    "-Wunused:_", // Warn unused
    "-Wmacros:after",
    "-Xlint:-byname-implicit,_", // Enable all linting options except lint-byname-implicit
    "-Wconf:any:wv",
    "-Wconf:src=akka-grpc/.*:silent",
    "-Wconf:src=src_managed/.*:silent",
    "-Ypatmat-exhaust-depth",
    "40"
//    "-Ymacro-debug-lite"
  )

  lazy val cq = taskKey[Unit]("Code quality")
  lazy val fmt = taskKey[Unit]("Code formatting")

  def createScalapbSettings(withGrpc: Boolean): Def.Setting[Seq[Target]] = {
    Compile / PB.targets += scalapb.gen(
      flatPackage = true,
      singleLineToProtoString = true,
      lenses = false,
      grpc = withGrpc
    ) -> (Compile / sourceManaged).value
  }

  def defineProject(projectName: String) = {
    Project(projectName, file(s"chopsticks-$projectName"))
      .settings(
        name := s"chopsticks-$projectName",
        version := {
          val useSnapshotVersion = sys.env.get("CHOPSTICKS_USE_SNAPSHOT_VERSION").map(_.toLowerCase).contains("true")
          val buildVersion = (version in ThisBuild).value
          if (useSnapshotVersion || !buildVersion.endsWith("-SNAPSHOT")) buildVersion
          else {
            val shortGitSha = sys.env.get("GITHUB_SHA").orElse(git.gitHeadCommit.value).get.take(8)
            s"${buildVersion.dropRight("-SNAPSHOT".length)}-$shortGitSha"
          }
        },
        Build.cq := {
          (Compile / scalafmtCheck).value
          (Test / scalafmtCheck).value
          val _ = (Compile / scalafmtSbtCheck).value
        },
        Build.fmt := {
          (Compile / scalafmt).value
          (Test / scalafmt).value
          (Compile / scalafmtSbt).value
        },
        Compile / compile / wartremoverErrors ++= Seq(
          //          Wart.Any,
          Wart.AnyVal,
          Wart.JavaSerializable,
          Wart.FinalCaseClass,
          Wart.FinalVal,
          Wart.JavaConversions,
          Wart.LeakingSealed,
          Wart.NonUnitStatements
//          Wart.Product
        ),
        wartremoverExcluded += sourceManaged.value,
        wartremoverExcluded += baseDirectory.value / "target" / "scala-2.13" / "akka-grpc",
        dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang"),
        libraryDependencies ++= Dependencies.scalatestDeps,
        Compile / doc / sources := Seq.empty,
        Compile / packageDoc / publishArtifact := false,
        ossPublishSettings
      )
  }

  def ossPublishSettings: Seq[Def.Setting[_]] = {
    import sbtrelease.ReleaseStateTransformations._
    import sbtrelease.ReleasePlugin.autoImport._
    Seq(
      releaseVersionBump := sbtrelease.Version.Bump.Minor,
      releaseIgnoreUntrackedFiles := true,
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/")),
      releaseProcess := Seq(
        checkSnapshotDependencies,
        inquireVersions,
        runClean,
        runTest,
        setReleaseVersion,
        commitReleaseVersion,
        tagRelease,
        releaseStepCommandAndRemaining("publish"),
        setNextVersion,
        commitNextVersion,
        pushChanges
      )
    )
  }
}
