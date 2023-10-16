package dev.chopsticks.sample.app

import akka.stream.scaladsl.Sink
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource.ZStreamToZAkkaSource
import dev.chopsticks.stream.ZStreamUtils
import zio.clock.Clock
import zio.duration._
import zio.{ExitCode, RIO, Schedule, ZIO}

import java.util.concurrent.atomic.AtomicInteger

object RetryStateTestApp extends ZAkkaApp {

  def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val schedule = Schedule.recurs(4) *> Schedule.exponential(100.millis)
    val count = new AtomicInteger(0)
    val task: RIO[Any with Clock, Int] = ZIO.effectSuspend {
      val i = count.getAndIncrement()
      println(s"Attempt $i...")

      val out =
        if (i < 6) {
          ZIO.fail(new IllegalStateException("Test failure"))
        }
        else {
          ZIO.succeed(123)
        }

      out.delay(1.second)
    }

    val app = ZStreamUtils
      .retry(task, schedule, ZIO.never)
      .toZAkkaSource()
      .killSwitch
      .interruptibleRunWith(Sink.foreach { output =>
        println(s"Output: $output")
      })

    app
      .orDie
      .as(ExitCode(0))
  }
}
