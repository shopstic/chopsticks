package dev.chopsticks.sample.app

import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import zio.duration._
import zio.{ExitCode, RIO, Schedule, UIO, ZIO, ZManaged}

object SafeRaceFirstTestApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    ZManaged
      .make(UIO(println("acquire"))) { _ =>
        UIO(println("release"))
      }
      .use { _ =>
        ZIO.effectSuspend(UIO(println("In use"))).repeat(Schedule.fixed(1.second))
          .onExit(e => UIO(println(s"Use exit $e")))
      }
      .unit
      .as(ExitCode(0))
  }
}
