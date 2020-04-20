package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, ZLogger}
import zio.{RIO, ZIO, ZLayer, ZManaged}

import scala.concurrent.duration._
import dev.chopsticks.fp.zio_ext._

import scala.concurrent.TimeoutException

object ZioTestApp extends AkkaApp {
  override type Env = AkkaApp.Env

  override protected def createEnv(untypedConfig: Config) = ZLayer.requires[AkkaApp.Env]

  private def testManaged = {
    ZManaged
      .makeInterruptible {
        ZLogger
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
  override def run: RIO[Env, Unit] = {
    testManaged.use { r => ZLogger.info(s"run with $r") }
  }
}
