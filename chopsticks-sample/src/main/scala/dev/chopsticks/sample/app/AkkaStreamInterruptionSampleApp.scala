package dev.chopsticks.sample.app

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, ZService}
import dev.chopsticks.fp.log_env.LogEnv
import dev.chopsticks.stream.ZAkkaStreams
import zio.{ZIO, ZLayer}

import scala.concurrent.duration._

object AkkaStreamInterruptionSampleApp extends AkkaApp {
  type Env = AkkaApp.Env

  protected def createEnv(untypedConfig: Config) = ZLayer.requires[AkkaApp.Env]

  def run: ZIO[Env, Throwable, Unit] = {
    val stream = ZAkkaStreams.interruptibleGraph(
      ZService[LogEnv.Service]
        .map(_.logger)
        .map { logger =>
          Source(1 to 10)
            .throttle(1, 1.second)
            .wireTap(n => logger.info(s"Out: $n"))
            .map { v =>
              if (v > 4) {
                throw new IllegalStateException("Die intentionally")
              }
              v
            }
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.fold(0)(_ + _))(Keep.both)
        },
      graceful = true
    )

    stream.unit
  }
}
