package dev.chopsticks.fp

import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import zio.{EnvironmentTag, Scope, ZIOAppArgs, ZLayer}

object ZPekkoApp {
  type ZAkkaAppEnv = HoconConfig with PekkoEnv with IzLogging
  type FullAppEnv = ZAkkaAppEnv with Scope
  //noinspection TypeAnnotation
  implicit val tag = zio.Tag[ZAkkaAppEnv]
  implicit val fullAppEnvTag = zio.Tag[FullAppEnv]
}

trait ZPekkoApp extends zio.ZIOApp {
  override type Environment = ZAkkaAppEnv

  override val environmentTag: EnvironmentTag[Environment] = EnvironmentTag[ZAkkaAppEnv]

  // todo add tracing and logger and possibly executor
  override val bootstrap: ZLayer[ZIOAppArgs, Throwable, Environment] = {
    val hoconConfigLayer = HoconConfig.live(Some(this.getClass))
    val pekkoEnvLayer = PekkoEnv.live()
    val izLoggingRouterLayer = IzLoggingRouter.live
    val izLoggingLayer = IzLogging.live()

    val layer1: ZLayer[Any, Throwable, HoconConfig with PekkoEnv with IzLoggingRouter] =
      hoconConfigLayer >+> (pekkoEnvLayer ++ izLoggingRouterLayer)
    val layer2: ZLayer[Any, Throwable, HoconConfig with PekkoEnv with IzLogging] =
      layer1 >+> izLoggingLayer
    layer2
  }
}

// todo add Tracing markings, set execution context and so on
//trait ZPekkoApp {
//  def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode]
//
//  def runtimeLayer: ZLayer[Any, Throwable, ZAkkaAppEnv] = {
//    val hoconConfigLayer = HoconConfig.live(Some(this.getClass))
//    val pekkoEnvLayer = PekkoEnv.live()
//    val izLoggingRouterLayer = IzLoggingRouter.live
//    val izLoggingLayer = IzLogging.live()
//
//    val layer1: ZLayer[Any, Throwable, HoconConfig with PekkoEnv with IzLoggingRouter] =
//      hoconConfigLayer >+> (pekkoEnvLayer ++ izLoggingRouterLayer)
//    val layer2: ZLayer[Any, Throwable, HoconConfig with PekkoEnv with IzLoggingRouter with IzLogging] =
//      layer1 >+> izLoggingLayer
//    layer2
//  }
//
//  final def main(commandArgs: Array[String]): Unit = {
//    val bootstrapPlatform = PlatformUtils.create(
//      corePoolSize = 0,
//      maxPoolSize = Runtime.getRuntime.availableProcessors(),
//      keepAliveTimeMs = 5000,
//      threadPoolName = "zio-app-bootstrap"
//    )
//    val bootstrapRuntime = zio.Runtime[Console](Has(Console.Service.live), bootstrapPlatform)
//
//    val main = for {
//      actorSystem <- PekkoEnv.actorSystem
//      exitCode <- run(commandArgs.toList)
//        .on(actorSystem.dispatcher)
//        .interruptibleRace(
//          // This will win the race when the actor system crashes outright
//          // without going through CoordinatedShutdown
//          ZIO
//            .fromFuture { _ =>
//              actorSystem.whenTerminated
//            }
//            .as(ExitCode(234))
//        )
//    } yield exitCode
//
//    val isShuttingDown = new AtomicBoolean(false)
//
//    try {
//      val exitCode = bootstrapRuntime.unsafeRun {
//        //noinspection SimplifyBuildUseInspection
//        for {
//          fiber <- main
//            .provideLayer(runtimeLayer)
//            .catchAllTrace { case (e, maybeTrace) =>
//              ZIO.succeed {
//                e.printStackTrace()
//                maybeTrace.foreach { t =>
//                  System.err.println("\n" + ZTraceConcisePrinter.prettyPrint(t))
//                }
//              }.as(ExitCode(1))
//            }
//            .fork
//          _ <- ZIO.succeed(java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
//            override def run(): Unit = {
//              isShuttingDown.set(true)
//              val _ = bootstrapRuntime.unsafeRunSync(fiber.interrupt)
//            }
//          }))
//          result <- fiber.join
//          _ <- fiber.interrupt
//        } yield result.code
//      }
//
//      sys.exit(exitCode)
//    }
//    catch {
//      case e: FiberFailure if e.cause.interruptedOnly && isShuttingDown.get() =>
//      case NonFatal(e) =>
//        bootstrapRuntime.platform.reportFatal(e)
//    }
//  }
//}
