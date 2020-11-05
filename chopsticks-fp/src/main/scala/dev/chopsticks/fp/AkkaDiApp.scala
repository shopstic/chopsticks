package dev.chopsticks.fp

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.fp.AkkaDiApp.AkkaDiAppContext
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.DiModule
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio._
import zio.internal.tracing.TracingConfig

import scala.util.Try
import scala.util.control.NonFatal
import distage.ModuleDef
import izumi.distage.model.definition
import zio.blocking.Blocking
import zio.clock.Clock
import zio.random.Random

object AkkaDiApp {
  type Env = Clock with zio.console.Console with zio.system.System with Random with Blocking with AkkaEnv

  object Env {
    def live(implicit actorSystem: ActorSystem): ULayer[Env] = {
      AkkaEnv.live(
        actorSystem
      ) >>> (Clock.live ++ zio.console.Console.live ++ zio.system.System.live ++ Random.live ++ Blocking.live ++ AkkaEnv.any)
    }

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    def createModule(implicit actorSystem: ActorSystem): definition.Module = new ModuleDef {
      make[AkkaEnv.Service].fromHas(AkkaEnv.live(actorSystem))
      make[Clock.Service].fromHas(Clock.live)
      make[zio.console.Console.Service].fromHas(zio.console.Console.live)
      make[zio.system.System.Service].fromHas(zio.system.System.live)
      make[Random.Service].fromHas(Random.live)
      make[Blocking.Service].fromHas(Blocking.live)
    }
  }

  final case class AkkaDiAppContext(
    actorSystem: ActorSystem,
    appConfig: Config,
    izLogging: IzLogging.Service,
    buildEnv: Task[DiEnv[AppEnv]]
  )

  private val factoryRuntime = Runtime((), zio.internal.Platform.global)

  def createRuntime[R <: AkkaEnv with IzLogging](
    layer: ZLayer[Any, Any, R],
    tracingConfig: TracingConfig = TracingConfig.enabled
  ): zio.Runtime[R] = {
    val task = ZIO.environment[R].map { r =>
      val akkaService = ZService.get[AkkaEnv.Service](r)
      val loggerService = ZService.get[IzLogging.Service](r)

      val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaService.actorSystem)
      val platform: zio.internal.Platform = new zio.internal.Platform.Proxy(
        zio.internal.Platform
          .fromExecutionContext(akkaService.dispatcher)
          .withTracingConfig(tracingConfig)
      ) {
        private val isShuttingDown = new AtomicBoolean(false)

        override def reportFailure(cause: Cause[Any]): Unit = {
          if (cause.died && shutdown.shutdownReason().isEmpty && isShuttingDown.compareAndSet(false, true)) {
            loggerService.logger.error("Application failure:\n" + cause.prettyPrint)
            val _ = shutdown.run(JvmExitReason)
          }
        }
      }

      Runtime(r, platform)
    }

    factoryRuntime.unsafeRun(task.provideLayer(layer))
  }
}

trait AkkaDiApp[Cfg] {

  type AppConfig = Has[Cfg]

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def createIzLogging(config: Config): IzLogging.Service = IzLogging.create(config)

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

  def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Cfg,
    allConfig: Config
  ): Task[DiEnv[AppEnv]]

  def create: AkkaDiAppContext = {
    val allConfig = unsafeLoadUntypedConfig
    val izLogging = createIzLogging(allConfig)
    val akkaActorSystem = createActorSystem(appName, allConfig)

    AkkaDiAppContext(
      akkaActorSystem,
      allConfig,
      izLogging,
      for {
        appConfig <- config(allConfig)
        akkaAppDi = AkkaDiApp.Env.createModule(akkaActorSystem)
        runEnv <-
          liveEnv(akkaAppDi ++ DiLayers(ZLayer.succeed(izLogging), ZIO.environment[AppEnv]), appConfig, allConfig)
      } yield runEnv
    )
  }

  def main(args: Array[String]): Unit = {
    val AkkaDiAppContext(akkaActorSystem, allConfig, izLogging, buildEnv) = create
    val zioTracingEnabled = Try(allConfig.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val runtime = AkkaDiApp.createRuntime(
      AkkaEnv.live(akkaActorSystem) ++ ZLayer.succeed(izLogging),
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )
    val shutdown = CoordinatedShutdown(akkaActorSystem)

    val main = for {
      runEnv <- buildEnv
      appFib <- runEnv
        .run(ZIO.accessM[AppEnv](_.get), args.headOption.contains("--dump-di-graph"))
        .fork
      appInterruptionPromise <- Promise.make[Nothing, Unit]
      _ <- UIO {
        shutdown.addTask("app-interruption", "interrupt app") { () =>
          runtime.unsafeRunToFuture(appInterruptionPromise.completeWith(ZIO.unit).as(Done))
        }
        shutdown.addJvmShutdownHook { () =>
          izLogging.logger.router.close()
        }
      }
      result <- {
        val interruptTask = appInterruptionPromise
          .await
          .raceFirst(Task.fromFuture(_ => akkaActorSystem.whenTerminated))
          .as(1)
          .ensuring(appFib.interrupt.ignore *> UIO(Done))
        appFib.join.raceFirst(interruptTask)
      }
    } yield result

    try {
      val exitCode = runtime.unsafeRun(main)
      sys.exit(exitCode)
    }
    catch {
      case NonFatal(e) =>
        runtime.platform.reportFailure(Die(e))
    }
  }
}
