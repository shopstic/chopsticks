import com.timushev.sbt.updates.UpdatesPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import wartremover.WartRemover.autoImport._
import sbt._
import sbt.Keys._

//noinspection TypeAnnotation
object Build {
  val buildVersion = "2.4.5"

  lazy val ITest = config("it") extend Test

  val ItTagName = "dev.chopsticks.test.tags.IntegrationTest"

  val forkTests = sys.env.get("FORK_TESTS").forall(_ == "true")

  val scalacLintingOptions = Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-explaintypes", // Explain type errors in more detail.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xlint:_",
    "-Wdead-code", // Warn when dead code is identified.
//    "-Werror", // Fail the compilation if there are any warnings.
    "-Wextra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Wnumeric-widen", // Warn when numerics are widened.
    "-Woctal-literal", // Warn on obsolete octal syntax.
//    "-Wself-implicit", // Warn when an implicit resolves to an enclosing self-definition.
    "-Wunused:imports", // Warn if an import selector is not referenced.
    "-Wunused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Wunused:privates", // Warn if a private member is unused.
    "-Wunused:locals", // Warn if a local definition is unused.
    "-Wunused:explicits", // Warn if an explicit parameter is unused.
    "-Wunused:implicits", // Warn if an implicit parameter is unused.
    "-Wunused:params", // Enable -Wunused:explicits,implicits.
    "-Wunused:linted", // -Xlint:unused.
    "-Wvalue-discard" // Warn when non-Unit expression results are unused.
  ) ++ scala.sys.env.get("FATAL_WARNINGS").map(_ => Seq("-Xfatal-warnings")).getOrElse(Seq.empty[String])

  val javacOptions = Seq("-encoding", "UTF-8")
  val scalacOptions = Seq(
    "-unchecked",
    "-feature",
    "-encoding",
    "utf-8"
//    "-Xfuture",
//    "-Ycache-plugin-class-loader:last-modified",
//    "-Ycache-macro-class-loader:last-modified",
//    "-Ybackend-parallelism",
//    Math.min(16, java.lang.Runtime.getRuntime.availableProcessors()).toString
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
