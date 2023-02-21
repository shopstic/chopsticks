package dev.chopsticks.sample.app

import dev.chopsticks.stream.ZStreamUtils
import zio.{Duration, ExitCode, RIO, Schedule, Scope, Task, ZIO, ZIOAppArgs}

import java.util.concurrent.atomic.AtomicInteger

object RetryStateTestApp extends zio.ZIOAppDefault {

  def run: RIO[Environment with ZIOAppArgs with Scope, Any] = {
    val schedule = Schedule.recurs(4) *> Schedule.exponential(Duration.fromMillis(100L))
    val count = new AtomicInteger(0)
    val task: Task[Int] = ZIO.suspend {
      val i = count.getAndIncrement()
      println(s"Attempt $i...")

      val out =
        if (i < 6) {
          ZIO.fail(new IllegalStateException("Test failure"))
        }
        else {
          ZIO.succeed(123)
        }

      out.delay(zio.Duration.fromSeconds(1L))
    }

    val app = ZStreamUtils
      .retry(task, schedule)
      .runForeach { output =>
        ZIO.succeed(println(s"Output: $output"))
      }

    app.as(ExitCode(0))
  }
}
