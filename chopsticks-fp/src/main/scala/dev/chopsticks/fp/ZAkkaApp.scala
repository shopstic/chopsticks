package dev.chopsticks.fp

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.PlatformUtils
import dev.chopsticks.fp.zio_ext._
import zio.{ExitCode, FiberFailure, IO, RIO, Task, ZEnv, ZLayer}

import java.util.concurrent.atomic.AtomicBoolean
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

  final def main(commandArgs: Array[String]): Unit = {
    val bootstrapPlatform = PlatformUtils.create(
      corePoolSize = 0,
      maxPoolSize = Runtime.getRuntime.availableProcessors(),
      keepAliveTimeMs = 5000,
      threadPoolName = "zio-app-bootstrap"
    )
    val bootstrapRuntime = zio.Runtime((), bootstrapPlatform)

    val main = for {
      actorSystem <- AkkaEnv.actorSystem
      appFib <- run(commandArgs.toList)
        .on(actorSystem.dispatcher)
        .fork
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

    val isShuttingDown = new AtomicBoolean(false)

    try {
      val exitCode = bootstrapRuntime.unsafeRun {
        //noinspection SimplifyBuildUseInspection
        for {
          fiber <- main
            .interruptAllChildrenPar
            .provideLayer(runtimeLayer)
            .fork
          _ <- IO.effectTotal(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
            override def run(): Unit = {
              isShuttingDown.set(true)
              val _ = bootstrapRuntime.unsafeRunSync(fiber.interrupt)
            }
          }))
          result <- fiber.join
          _ <- fiber.interrupt
        } yield result.code
      }

      sys.exit(exitCode)
    }
    catch {
      case e: FiberFailure if e.cause.interruptedOnly && isShuttingDown.get() =>
      case NonFatal(e) =>
        bootstrapRuntime.platform.reportFatal(e)
    }
  }
}
