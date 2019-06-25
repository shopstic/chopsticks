package dev.chopsticks.fp
import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import kamon.Kamon
import kamon.system.SystemMetrics
import pureconfig.{KebabCase, PascalCase}
import scalaz.zio
import scalaz.zio.Exit.Cause.Traced
import scalaz.zio.blocking.Blocking
import scalaz.zio.clock.Clock
import scalaz.zio.console.Console
import scalaz.zio.internal.{Platform, PlatformLive}
import scalaz.zio.random.Random
import scalaz.zio.system.System
import scalaz.zio.{FiberFailure, IO, TaskR, UIO, ZManaged, system}

import scala.util.Try

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

  protected def run: TaskR[Env, Unit]

  protected def setupMetrics(config: Config): Unit = {
    Kamon.reconfigure(config)
    Kamon.loadReportersFromConfig()
    SystemMetrics.startCollecting()
  }

  def main(args: Array[String]): Unit = {
    val appName = KebabCase.fromTokens(PascalCase.toTokens(this.getClass.getSimpleName.replaceAllLiterally("$", "")))
    val appConfigName = this.getClass.getPackage.getName.replaceAllLiterally(".", "/") + "/" + appName
    val config = ConfigFactory.load(
      appConfigName,
      ConfigParseOptions.defaults.setAllowMissing(false),
      ConfigResolveOptions.defaults
    )

    setupMetrics(config)

    val as = createActorSystem(appName, config)
    val managedEnv: ZManaged[AkkaApp.Env, Nothing, Env] = createEnv(config)
    val runtime: zio.Runtime[AkkaApp.Env] = new zio.Runtime[AkkaApp.Env] {
      val Environment: AkkaApp.Env = new AkkaApp.LiveEnv {
        implicit val actorSystem: ActorSystem = as
      }
      val Platform: Platform = PlatformLive.fromExecutionContext(as.dispatcher)
    }

    val app = managedEnv
      .use { e =>
        run.provide(e) *> UIO(0)
      }
      .catchAll { e =>
        ZLogger.error(s"App failed: ${e.getMessage}", e) *> UIO(1)
      }

    val exitCode = Try {
      runtime.unsafeRun(
        for {
          fiber <- app.fork
          _ <- IO.effectTotal(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
            override def run(): Unit = {
              val _ = runtime.unsafeRunSync(fiber.interrupt)
            }
          }))
          result <- fiber.join
        } yield result
      )
    }.recover {
        case FiberFailure(Traced(cause, trace)) =>
          println(s"Main thread failed with FiberFailure: $cause")
          println(trace.prettyPrint)
          1
        case FiberFailure(cause) =>
          println(s"Main thread failed with FiberFailure")
          println(cause.prettyPrint)
          1
        case e =>
          println(s"Main thread failed with: $e")
          1
      }
      .getOrElse(1)

    sys.exit(exitCode)
  }
}
