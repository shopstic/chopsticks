package dev.chopsticks.sample.app

import org.apache.pekko.stream.scaladsl.Sink
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource.ZStreamToZAkkaSource
import dev.chopsticks.stream.ZStreamUtils
import zio.{durationInt, RIO, Schedule, ZIO}

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn

object RetryStateTestApp extends ZPekkoApp {

  @nowarn("cat=lint-infer-any")
  def run: RIO[ZAkkaAppEnv, Any] = {
    val schedule = Schedule.recurs(4) *> Schedule.exponential(100.millis)
    val count = new AtomicInteger(0)
    val task: RIO[Any, Int] = ZIO.suspend {
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

    app.orDie
  }
}
