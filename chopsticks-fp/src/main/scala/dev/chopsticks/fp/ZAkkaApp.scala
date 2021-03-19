package dev.chopsticks.fp

import akka.Done
import akka.actor.CoordinatedShutdown
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.PlatformUtils
import zio.{ExitCode, RIO, Task, UIO, ZEnv, ZLayer}

import scala.concurrent.Future
import scala.util.control.NonFatal

object ZAkkaApp {
  type ZAkkaAppEnv = ZEnv with HoconConfig with AkkaEnv with IzLogging
}

trait ZAkkaApp {
  def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode]

  def runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
    val zEnvLayer = ZEnv.live
    val hoconConfigLayer = HoconConfig.live(this.getClass)
    val akkaEnvLayer = AkkaEnv.live()
    val izLoggingLayer = IzLogging.live()

    hoconConfigLayer ++ (hoconConfigLayer >>> akkaEnvLayer) ++ (hoconConfigLayer >>> izLoggingLayer) ++ zEnvLayer
  }

  final def main(args0: Array[String]): Unit = {
    val bootstrapPlatform = PlatformUtils.create(
      corePoolSize = 0,
      maxPoolSize = Runtime.getRuntime.availableProcessors(),
      keepAliveTimeMs = 5000,
      threadPoolName = "zio-app-bootstrap"
    )
    val bootstrapRuntime = zio.Runtime((), bootstrapPlatform)

    val main = for {
      actorSystem <- AkkaEnv.actorSystem
      shutdown = CoordinatedShutdown(actorSystem)
      logger <- IzLogging.logger
      appFib <- run(args0.toList)
        .on(actorSystem.dispatcher)
        .fork
      _ <- UIO {
        shutdown.addTask("app-interruption", "interrupt app") { () =>
          bootstrapRuntime.unsafeRunToFuture(appFib.interrupt.as(Done))
        }
        shutdown.addTask("before-actor-system-terminate", "close IzLogging router") { () =>
          Future.successful {
            logger.router.close()
            Done
          }
        }
      }
      exitCode <- appFib
        .join
        .raceFirst(
          // This will win the race when the actor system crashes outright
          // without going through CoordinatedShutdown
          Task
            .fromFuture { _ =>
              actorSystem.whenTerminated
            }
            .as(ExitCode(234))
        )
    } yield exitCode

    try {
      val exitCode = bootstrapRuntime.unsafeRun(main.provideLayer(runtimeLayer))
      sys.exit(exitCode.code)
    }
    catch {
      case NonFatal(e) =>
        bootstrapRuntime.platform.reportFatal(e)
    }
  }
}
