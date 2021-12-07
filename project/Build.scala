import com.jsuereth.sbtpgp.PgpKeys
import com.timushev.sbt.updates.UpdatesPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import protocbridge.Target
import wartremover.WartRemover.autoImport._
import sbt.{Def, _}
import sbt.Keys._
import sbtprotoc.ProtocPlugin.autoImport.PB
import xerial.sbt.Sonatype.autoImport._

//noinspection TypeAnnotation
object Build {

  lazy val ITest = config("it") extend Test

  val ItTagName = "dev.chopsticks.test.tags.IntegrationTest"

  val forkTests = sys.env.get("FORK_TESTS").forall(_ == "false")

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
        version := (ThisBuild / version).value,
        publishMavenStyle := (ThisBuild / publishMavenStyle).value,
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
        libraryDependencies ++= Dependencies.scalatestDeps
//        ossPublishSettings
      )
  }

  /*def ossPublishSettings: Seq[Def.Setting[_]] = {
    import sbtrelease.ReleaseStateTransformations._
    import sbtrelease.ReleasePlugin.autoImport._
    Seq(
      releaseVersionBump := sbtrelease.Version.Bump.Minor,
      releaseIgnoreUntrackedFiles := true,
      autoAPIMappings := true,
      publishMavenStyle := true,
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/")),
      homepage := Some(url("https://github.com/shopstic/chopsticks")),
      releasePublishArtifactsAction := PgpKeys.publishSigned.value,
      pomIncludeRepository := { _ =>
        false
      },
      scmInfo := Some(
        ScmInfo(
          url("https://github.com/shopstic/chopsticks"),
          "scm:git:https://github.com/shopstic/chopsticks.git"
        )
      ),
      sonatypeProfileName := "dev.chopsticks",
      sonatypeRepository := "https://s01.oss.sonatype.org/service/local",
      sonatypeCredentialHost := "s01.oss.sonatype.org",
      developers := List(
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
      ),
      releaseProcess := {
        Seq[ReleaseStep](
          checkSnapshotDependencies,
//          ReleaseStep(
//            action = st => {
//              setupGpg()
//              st
//            }
//          ),
          inquireVersions,
          // publishing locally so that the pgp password prompt is displayed early
          // in the process
          releaseStepCommandAndRemaining("+publishLocalSigned"),
          runClean,
//          runTest,
          setReleaseVersion,
          commitReleaseVersion,
          tagRelease,
          releaseStepCommandAndRemaining("+publishSigned"),
          releaseStepCommand("sonatypeBundleRelease"),
          setNextVersion,
          commitNextVersion,
          pushChanges
        )
      }
    )
  }

  def setupGpg(): Unit = {
    import scala.sys.process._
    val secret = sys.env("PGP_SECRET")
    val _ = (s"echo $secret" #| "base64 --decode" #| "gpg --batch --import").!!
  }*/

}
