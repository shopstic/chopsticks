import com.timushev.sbt.updates.UpdatesPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import wartremover.WartRemover.autoImport._
import sbt._
import sbt.Keys._

//noinspection TypeAnnotation
object Build {
  val buildVersion = "2.11.1"

  lazy val ITest = config("it") extend Test

  val ItTagName = "dev.chopsticks.test.tags.IntegrationTest"

  val forkTests = sys.env.get("FORK_TESTS").forall(_ == "true")

  val scalacLintingOptions = Seq(
    "-explaintypes", // Explain type errors in more detail.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Wdead-code", // Warn when dead code is identified.
    "-Wextra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Wnumeric-widen", // Warn when numerics are widened.
    "-Wvalue-discard", // Warn when non-Unit expression results are unused.
    "-Wunused:_", // Warn unused
    "-Xlint:_", // Enable all linting options
    "-Wconf:any:wv"
  ) ++ scala.sys.env.get("FATAL_WARNINGS").map(_ => Seq("-Werror")).getOrElse(Seq.empty[String])

  val javacOptions = Seq("-encoding", "UTF-8")
  val scalacOptions = Seq(
    "-encoding",
    "utf-8"
  ) ++ scala.sys.env
    .get("SCALAC_OPTIMIZE")
    .map(_.split(" ").toVector)
    .getOrElse(Vector.empty[String]) ++ scalacLintingOptions

  lazy val cq = taskKey[Unit]("Code quality")
  lazy val fmt = taskKey[Unit]("Code formatting")

  def defineProject(projectName: String) = {
    Project(projectName, file(s"chopsticks-$projectName"))
      .settings(
        name := s"chopsticks-$projectName",
        version := buildVersion,
        Build.cq := {
          (Compile / scalafmtCheck).value
          (Test / scalafmtCheck).value
          (Compile / scalafmtSbtCheck).value
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
        dependencyUpdatesFilter -= moduleFilter(organization = "org.scala-lang"),
        libraryDependencies ++= Dependencies.scalatestDeps,
        Compile / doc / sources := Seq.empty,
        Compile / packageDoc / publishArtifact := false
      )
  }
}
