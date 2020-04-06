package dev.chopsticks.fp
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.LogEnv
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.Platform
import zio.internal.tracing.TracingConfig
import zio.random.Random

import scala.util.Try
import scala.util.control.NonFatal

object AkkaApp extends LoggingContext {
  type Env = Clock with Console with zio.system.System with Random with Blocking with AkkaEnv with LogEnv

  object Env {
    def live(implicit actorSystem: ActorSystem): ZLayer[Any, Nothing, Env] = {
      AkkaEnv.live(actorSystem) >>> (Clock.live ++ Console.live ++ zio.system.System.live ++ Random.live ++ Blocking.live ++ AkkaEnv.any ++ LogEnv.live)
    }
  }

  private val factoryRuntime = Runtime((), Platform.global)

  def createRuntime[R <: AkkaEnv with LogEnv](
    layer: ZLayer[Any, Any, R],
    tracingConfig: TracingConfig = TracingConfig.enabled
  ): zio.Runtime[R] = {
    val task = ZIO.environment[R].map { r =>
      val akkaService = r.get[AkkaEnv.Service]
      val loggerService = r.get[LogEnv.Service]

      val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaService.actorSystem)
      val platform: Platform = new zio.internal.Platform.Proxy(
        Platform
          .fromExecutionContext(akkaService.dispatcher)
          .withTracingConfig(tracingConfig)
      ) {
        private val isShuttingDown = new AtomicBoolean(false)

        override def reportFailure(cause: Cause[Any]): Unit = {
          if (!cause.interrupted && shutdown.shutdownReason.isEmpty && isShuttingDown.compareAndSet(false, true)) {
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

trait AkkaApp extends LoggingContext {
  type Env <: AkkaApp.Env

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def createEnv(untypedConfig: Config): ZLayer[AkkaApp.Env, Throwable, Env]

  def run: RIO[Env, Unit]

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replaceAllLiterally("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replaceAllLiterally(".", "/") + "/" + appName
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
    val appLayer = createEnv(config)
    val zioTracingEnabled = Try(config.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)

    val runtime: zio.Runtime[AkkaApp.Env] = AkkaApp.createRuntime(
      AkkaApp.Env.live(akkaActorSystem),
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )

    val main = for {
      appFib <- run
        .ensuring(ZIO.interruptAllChildren)
        .provideLayer(appLayer)
        .fork
      _ <- UIO {
        shutdown.addTask("app-interruption", "interrupt app") { () =>
          runtime.unsafeRunToFuture(appFib.interrupt.ignore *> UIO(Done))
        }
      }
      result <- appFib.join
    } yield result

    try {
      runtime.unsafeRun(main)
      sys.exit(0)
    }
    catch {
      case NonFatal(e) =>
        runtime.platform.reportFailure(Die(e))
    }
  }
}
