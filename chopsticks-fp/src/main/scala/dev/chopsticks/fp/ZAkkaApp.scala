package dev.chopsticks.fp

import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.fp.util.{PlatformUtils, ZTraceConcisePrinter}
import dev.chopsticks.fp.zio_ext._
import zio.console.Console
import zio.{ExitCode, FiberFailure, Has, IO, RIO, Task, UIO, ZEnv, ZLayer}

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

object ZAkkaApp {
  type ZAkkaAppEnv = ZEnv with HoconConfig with AkkaEnv with IzLogging
  // noinspection TypeAnnotation
  implicit val tag = zio.Tag[ZAkkaAppEnv]
}

trait ZAkkaApp {
  def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode]

  def runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
    val zEnvLayer = ZEnv.live
    val hoconConfigLayer = HoconConfig.live(Some(this.getClass))
    val akkaEnvLayer = AkkaEnv.live()
    val izLoggingRouterLayer = IzLoggingRouter.live
    val izLoggingLayer = IzLogging.live()

    (hoconConfigLayer >+> (akkaEnvLayer ++ izLoggingRouterLayer) >+> izLoggingLayer) ++ zEnvLayer
  }

  final def main(commandArgs: Array[String]): Unit = {
    val bootstrapPlatform = PlatformUtils.create(
      corePoolSize = 0,
      maxPoolSize = Runtime.getRuntime.availableProcessors(),
      keepAliveTimeMs = 5000,
      threadPoolName = "zio-app-bootstrap"
    )
    val bootstrapRuntime = zio.Runtime[Console](Has(Console.Service.live), bootstrapPlatform)

    val main = for {
      actorSystem <- AkkaEnv.actorSystem
      exitCode <- run(commandArgs.toList)
        .on(actorSystem.dispatcher)
        .interruptibleRace(
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
        // noinspection SimplifyBuildUseInspection
        for {
          fiber <- main
            .provideLayer(runtimeLayer)
            .catchAllTrace { case (e, maybeTrace) =>
              UIO {
                e.printStackTrace()
                maybeTrace.foreach { t =>
                  System.err.println("\n" + ZTraceConcisePrinter.prettyPrint(t))
                }
              }.as(ExitCode(1))
            }
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
