package dev.chopsticks.fp
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.{KebabCase, PascalCase}
import zio.Cause.Die
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.PlatformLive
import zio.internal.tracing.TracingConfig
import zio.random.Random
import zio.system.System

import scala.util.Try
import scala.util.control.NonFatal

object AkkaApp extends LoggingContext {
  type Env = Clock with Console with system.System with Random with Blocking with AkkaEnv with LogEnv
  trait LiveEnv
      extends Clock.Live
      with Console.Live
      with System.Live
      with Random.Live
      with Blocking.Live
      with LogEnv.Live
      with AkkaEnv

  object Env {
    final case class Live(actorSystem: ActorSystem) extends LiveEnv {
      val akkaService: AkkaEnv.Service = AkkaEnv.Service.Live(actorSystem)
    }
  }

  def createRuntime[R <: AkkaEnv with LogEnv](
    env: R,
    tracingConfig: TracingConfig = TracingConfig.enabled
  ): zio.Runtime[R] = {
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(env.akkaService.actorSystem)

    new zio.Runtime[R] {
      val Platform: zio.internal.Platform = new zio.internal.Platform.Proxy(
        PlatformLive
          .fromExecutionContext(env.akkaService.actorSystem.dispatcher)
          .withTracingConfig(tracingConfig)
      ) {
        private val isShuttingDown = new AtomicBoolean(false)

        override def reportFailure(cause: Cause[Any]): Unit = {
          if (!cause.interrupted && shutdown.shutdownReason.isEmpty && isShuttingDown.compareAndSet(false, true)) {
            Environment.logger.error("Application failure:\n" + cause.prettyPrint)
            val _ = shutdown.run(JvmExitReason)
          }
        }
      }
      val Environment: R = env
    }
  }
}

trait AkkaApp extends LoggingContext {
  type Env <: AkkaApp.Env

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def createEnv(untypedConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env]

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
    val akkaActorSystem = createActorSystem(appName, config)
    val managedEnv: ZManaged[AkkaApp.Env, Nothing, Env] = createEnv(config)
    val zioTracingEnabled = Try(config.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)
    val runtime: zio.Runtime[AkkaApp.Env] = AkkaApp.createRuntime(
      AkkaApp.Env.Live(akkaActorSystem),
      if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled
    )

    val main = for {
      appFib <- managedEnv.use(run.provide(_)).fork
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
    } catch {
      case NonFatal(e) =>
        runtime.Platform.reportFailure(Die(e))
    }
  }
}
