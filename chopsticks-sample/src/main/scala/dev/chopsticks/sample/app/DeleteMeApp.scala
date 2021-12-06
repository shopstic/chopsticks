package dev.chopsticks.sample.app

import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.util.LoggedRace
import zio.duration._
import zio.{ExitCode, RIO, Schedule, UIO, ZIO}

import java.time.LocalDateTime

object DeleteMeApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    LoggedRace()
      .add("one", ZIO.fail(new IllegalStateException("test death")).delay(5.seconds))
      .add("two", ZIO.effectSuspend(UIO(println(s"two now = ${LocalDateTime.now}"))).repeat(Schedule.fixed(1.second)))
      .add(
        "three",
        ZIO.effectSuspend(UIO(println(s"three now = ${LocalDateTime.now}"))).repeat(Schedule.fixed(1.second))
      )
      .run()
      .as(ExitCode(0))
  }
}
