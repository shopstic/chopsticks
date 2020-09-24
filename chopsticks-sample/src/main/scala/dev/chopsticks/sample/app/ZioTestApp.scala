package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.AkkaApp
import zio.{RIO, ZIO, ZLayer, ZManaged}

import scala.concurrent.duration._
import dev.chopsticks.fp.zio_ext._

import scala.concurrent.TimeoutException

object ZioTestApp extends AkkaApp {
  override type Env = AkkaApp.Env with IzLogging

  override protected def createEnv(untypedConfig: Config) =
    ZLayer.requires[AkkaApp.Env] ++ IzLogging.live(untypedConfig)

  private def testManaged = {
    for {
      logger <- ZManaged.access[IzLogging](_.get).map(_.ctxZioLogger)
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

  override def run: RIO[Env, Unit] = {
    testManaged.use { r =>
      for {
        logger <- ZIO.access[IzLogging](_.get).map(_.ctxZioLogger)
        _ <- logger.info(s"run with $r")
      } yield ()
    }
  }

}
