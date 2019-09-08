package dev.chopsticks.fp
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.{KebabCase, PascalCase}
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

object AkkaApp {
  type Env = Clock with Console with system.System with Random with Blocking with AkkaEnv with LogEnv
  trait LiveEnv
      extends Clock.Live
      with Console.Live
      with System.Live
      with Random.Live
      with Blocking.Live
      with LogEnv.Live
      with AkkaEnv {
//    override val blocking: Blocking.Service[Any] = new Blocking.Service[Any] {
//      lazy val blockingExecutor: ZIO[Any, Nothing, Executor] = UIO(
//        Executor.fromExecutionContext(Int.MaxValue)(
//          actorSystem.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
//        )
//      )
//    }
  }
}

trait AkkaApp extends LoggingContext {
  type Env <: AkkaApp.Env

  protected def createActorSystem(appName: String, config: Config): ActorSystem = ActorSystem(appName, config)

  protected def createEnv(untypedConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env]

  protected def run: RIO[Env, Unit]

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replaceAllLiterally("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replaceAllLiterally(".", "/") + "/" + appName
    val config = ConfigFactory.load(
      appConfigName,
      ConfigParseOptions.defaults.setAllowMissing(false),
      ConfigResolveOptions.defaults
    )
    val akkaActorSystem = createActorSystem(appName, config)
    val managedEnv: ZManaged[AkkaApp.Env, Nothing, Env] = createEnv(config)
    val zioTracingEnabled = Try(config.getBoolean("zio.trace")).recover { case _ => true }.getOrElse(true)
    val shutdown: CoordinatedShutdown = CoordinatedShutdown(akkaActorSystem)
    val runtime: zio.Runtime[AkkaApp.Env] = new zio.Runtime[AkkaApp.Env] {
      val Environment: AkkaApp.Env = new AkkaApp.LiveEnv {
        implicit val actorSystem: ActorSystem = akkaActorSystem
      }
      val Platform: zio.internal.Platform = new zio.internal.Platform.Proxy(
        PlatformLive
          .fromExecutionContext(akkaActorSystem.dispatcher)
          .withTracingConfig(if (zioTracingEnabled) TracingConfig.enabled else TracingConfig.disabled)
      ) {
        private val isShuttingDown = new AtomicBoolean(false)
        override def reportFailure(cause: Cause[_]): Unit = {
          if (!cause.interrupted && shutdown.shutdownReason.isEmpty && isShuttingDown.compareAndSet(false, true)) {
            super.reportFailure(cause)
            val _ = shutdown.run(JvmExitReason)
          }
        }
      }
    }

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
      case NonFatal(_) =>
        sys.exit(1)
    }
  }
}
