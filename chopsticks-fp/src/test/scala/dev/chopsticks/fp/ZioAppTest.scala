package dev.chopsticks.fp
import dev.chopsticks.fp.iz_logging.{IzLogging, IzLoggingRouter}
import dev.chopsticks.fp.iz_logging.IzLogging.IzLoggingConfig
import logstage.Log
import zio.{ExitCode, Schedule, UIO, URIO, ZIO}

import java.time.LocalDateTime
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object ZioAppTest extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val task = for {
      zioLogger <- IzLogging.zioLogger
      _ <-
        ZIO.effectSuspend(zioLogger.info(s"Current time: ${LocalDateTime.now}")).repeat(Schedule.fixed(1.second.toJava))
    } yield ()

    task
      .as(ExitCode(0))
      .provideSomeLayer[zio.ZEnv](
        IzLoggingRouter.live >>> IzLogging.live(IzLoggingConfig(
          noColor = false,
          level = Log.Level.Debug,
          jsonFileSink = None
        ))
      )
      .catchAll(e => UIO(e.printStackTrace()).as(ExitCode(1)))
  }
}
