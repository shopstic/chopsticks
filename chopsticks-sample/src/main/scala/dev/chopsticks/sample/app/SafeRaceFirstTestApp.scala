package dev.chopsticks.sample.app

import cats.data.NonEmptyList
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.util.Race
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import zio.{durationInt, RIO, Schedule, Scope, ZIO}

import scala.annotation.nowarn

object SafeRaceFirstTestApp extends ZPekkoApp {
  @nowarn("cat=lint-infer-any")
  override def run: RIO[ZAkkaAppEnv with Scope, Any] = {
    Race(
      NonEmptyList
        .fromListUnsafe(List.tabulate(3) { i =>
          ZIO
            .suspend(ZIO.succeed(println(s"Par $i")))
            .repeat(Schedule.fixed(1.second).whileOutput(_ < 5))
            .unit
            .onInterrupt(_ =>
              ZIO.succeed(println(s"Par $i interrupting")) *>
                ZIO.succeed(println(s"Par $i interrupted")).delay(3.seconds)
            )
        })
    )
      .run()
      .interruptibleRace(ZIO
        .acquireRelease(ZIO.succeed(println("acquire"))) { _ =>
          ZIO.succeed(println("release"))
        }
        .zipRight {
          ZIO.suspend(ZIO.succeed(println("In use"))).repeat(Schedule.fixed(1.second).whileOutput(_ < 3))
            .onExit(e => ZIO.succeed(println(s"Use exit $e")))
        }
        .unit)
  }
}
