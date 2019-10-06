package dev.chopsticks.sample.app

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, LogEnv}
import dev.chopsticks.stream.ZAkkaStreams
import zio.{ZIO, ZManaged}

import scala.concurrent.duration._

object AkkaStreamInterruptionSampleApp extends AkkaApp {
  type Env = AkkaApp.Env

  protected def createEnv(untypedConfig: Config) = ZManaged.environment[AkkaApp.Env]

  def run: ZIO[Env, Throwable, Unit] = {
    val stream = ZAkkaStreams.interruptableGraph(
      ZIO.access[LogEnv] { env =>
        Source(1 to 10)
          .throttle(1, 1.second)
          .wireTap(n => env.logger.info(s"Out: $n"))
          .map { v =>
            if (v > 4) {
              throw new IllegalStateException("shit the bed")
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
