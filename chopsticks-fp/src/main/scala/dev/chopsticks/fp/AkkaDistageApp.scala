package dev.chopsticks.fp

import java.nio.file.Paths

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import distage.Injector
import izumi.distage.model.effect.DIEffect
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio._
import zio.internal.tracing.TracingConfig

import scala.util.Try
import scala.util.control.NonFatal

object AkkaDistageApp {
  type DiModule = izumi.distage.model.definition.Module
  private val zioDiEffect = implicitly[DIEffect[Task]]

  private object InterruptiableZioDiEffect extends DIEffect[Task] {
    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = zioDiEffect.flatMap[A, B](fa)(f)
    override def bracket[A, B](acquire: => Task[A])(release: A => Task[Unit])(use: A => Task[B]): Task[B] = {
      zioDiEffect.bracket[A, B](acquire)(release)(use)
    }

    override def bracketCase[A, B](
      acquire: => Task[A]
    )(release: (A, Option[Throwable]) => Task[Unit])(use: A => Task[B]): Task[B] = {
      println(s"bracketCase $acquire")
      zioDiEffect.bracketCase(acquire.interruptible)(release)(use)
    }
    override def maybeSuspend[A](eff: => A): Task[A] = zioDiEffect.maybeSuspend(eff)
    override def definitelyRecover[A](action: => Task[A])(recover: Throwable => Task[A]): Task[A] =
      zioDiEffect.definitelyRecover(action)(recover)
    override def definitelyRecoverCause[A](
      action: => Task[A]
    )(recoverCause: (Throwable, () => Throwable) => Task[A]): Task[A] = {
      zioDiEffect.definitelyRecoverCause(action)(recoverCause)
    }
    override def fail[A](t: => Throwable): Task[A] = zioDiEffect.fail(t)
    override def pure[A](a: A): Task[A] = zioDiEffect.pure(a)
    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = zioDiEffect.map(fa)(f)
    override def map2[A, B, C](fa: Task[A], fb: Task[B])(f: (A, B) => C): Task[C] = zioDiEffect.map2(fa, fb)(f)
  }
}

trait AkkaDistageApp extends LoggingContext {
  import AkkaDistageApp._

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def define(
    akkaAppModuleDef: DiModule,
    lbConfig: Config
  ): DiModule

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replace("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replace(".", "/") + "/" + appName
    val customAppConfig = scala.sys.props.get("config.file") match {
      case Some(customConfigFile) =>
        ConfigFactory
          .parseFile(Paths.get(customConfigFile).toFile, ConfigParseOptions.defaults().setAllowMissing(false))
          .resolve(ConfigResolveOptions.defaults())
      case None =>
        ConfigFactory.empty()
    }

    val config = customAppConfig.withFallback(
      ConfigFactory.load(
        appConfigName,
        ConfigParseOptions.defaults.setAllowMissing(false),
        ConfigResolveOptions.defaults
      )
    )

    if (!config.getBoolean("akka.coordinated-shutdown.run-by-jvm-shutdown-hook")) {
      throw new IllegalArgumentException(
        "'akka.coordinated-shutdown.run-by-jvm-shutdown-hook' is not set to 'on'. Check your HOCON application config."
      )
    }

    val akkaActorSystem = createActorSystem(appName, config)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)

    val zioTracingEnabled = Try(config.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val runtime = AkkaApp.createRuntime(
      AkkaEnv.live(akkaActorSystem) ++ LogEnv.live,
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )

    val akkaAppModule = AkkaApp.Env.createModule(akkaActorSystem)
    val moduleDef = define(akkaAppModule, config)

    implicit val diEff: DIEffect[Task] = InterruptiableZioDiEffect
    val run = Injector().produceRunF[Task, Unit](moduleDef)((_: Unit) => Task.unit)

    val main = for {
      appFib <- run
        .ensuring(ZIO.interruptAllChildren)
        .fork
      _ <- UIO {
        shutdown.addTask("app-interruption", "interrupt app") { () =>
          runtime.unsafeRunToFuture(appFib.interrupt.ignore *> UIO(Done))
        }
      }
      result <- appFib.join
    } yield result

    try {
      val _ = runtime.unsafeRun(main)
      sys.exit(0)
    }
    catch {
      case NonFatal(e) =>
        runtime.platform.reportFailure(Die(e))
    }
  }
}
