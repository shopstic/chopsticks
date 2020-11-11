package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiLayers}
import zio.{Task, ZIO, ZManaged}

import scala.concurrent.duration._
import dev.chopsticks.fp.zio_ext._

import scala.concurrent.TimeoutException

object ZioTestApp extends AkkaDiApp[Unit] {

  override def config(allConfig: Config) = Task.unit

  override def liveEnv(akkaAppDi: DiModule, appConfig: Unit, allConfig: Config) = {
    Task {
      LiveDiEnv(akkaAppDi ++ DiLayers(AppLayer(app)))
    }
  }

  private def testManaged = {
    for {
      logger <- ZManaged.access[IzLogging](_.get).map(_.zioLogger)
      _ <- {
        ZManaged
          .makeInterruptible {
            logger
              .info("works")
              .delay(4.seconds)
              .log("Test effect")
          } { _ => ZIO.succeed(()) }
          .timeout(1.second)
          .flatMap {
            case Some(e) =>
              println("Some")
              ZManaged.succeed(e)
            case None =>
              println("None")
              ZManaged.fail(new TimeoutException("Timed out"))
          }
      }
    } yield ()
  }

  def app = {
    testManaged.use { r =>
      for {
        logger <- ZIO.access[IzLogging](_.get).map(_.zioLogger)
        _ <- logger.info(s"run with $r")
      } yield ()
    }
  }

}
