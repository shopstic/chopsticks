package dev.chopsticks.fp

import java.nio.file.Paths

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.fp.DiEnv.DiModule
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio._
import zio.internal.tracing.TracingConfig

import scala.util.Try
import scala.util.control.NonFatal

trait AkkaDiApp extends LoggingContext {

  type Env <: Has[_]
  type Cfg
  type AppConfig = Has[Cfg]

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  def app: RIO[Env, Unit]

  def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Cfg,
    allConfig: Config
  ): Task[DiEnv[Env]]

  lazy val appName: String = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replace("$", "")))

  def unsafeLoadUntypedConfig: Config = {
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

    config
  }

  def config(allConfig: Config): Task[Cfg]

  def main(args: Array[String]): Unit = {
    val allConfig = unsafeLoadUntypedConfig

    val akkaActorSystem = createActorSystem(appName, allConfig)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)

    val zioTracingEnabled = Try(allConfig.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val runtime = AkkaApp.createRuntime(
      AkkaEnv.live(akkaActorSystem) ++ LogEnv.live,
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )

    val main = for {
      appConfig <- config(allConfig)
      akkaAppDi = AkkaApp.Env.createModule(akkaActorSystem)
      runEnv <- liveEnv(akkaAppDi, appConfig, allConfig)
      appFib <- runEnv
        .run(app, args.headOption.contains("--dump-di-graph"))
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
