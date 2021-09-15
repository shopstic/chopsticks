package dev.chopsticks.sample.app

import cats.data.NonEmptyList
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.util.Race
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import zio.duration._
import zio.{ExitCode, RIO, Schedule, UIO, ZIO, ZManaged}

object SafeRaceFirstTestApp extends ZAkkaApp {
  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {

    Race(NonEmptyList
      .fromListUnsafe(List.tabulate(3) { i =>
        ZIO
          .effectSuspend(UIO(println(s"Par $i")))
          .repeat(Schedule.fixed(1.second).whileOutput(_ < 5))
          .unit
          .onInterrupt(_ =>
            UIO(println(s"Par $i interrupting")) *> UIO(println(s"Par $i interrupted")).delay(3.seconds)
          )
      }))
      .run()
      .safeRaceFirst(ZManaged
        .make(UIO(println("acquire"))) { _ =>
          UIO(println("release"))
        }
        .use { _ =>
          ZIO.effectSuspend(UIO(println("In use"))).repeat(Schedule.fixed(1.second).whileOutput(_ < 3))
            .onExit(e => UIO(println(s"Use exit $e")))
        }
        .unit)
      .as(ExitCode(0))
  }
}
