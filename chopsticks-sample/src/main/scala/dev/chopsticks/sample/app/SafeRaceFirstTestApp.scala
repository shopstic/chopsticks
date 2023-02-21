package dev.chopsticks.sample.app

import dev.chopsticks.fp.util.Race
import dev.chopsticks.fp.zio_ext.*
import zio.{Duration, ExitCode, RIO, Schedule, Scope, ZIO, ZIOAppArgs}
import zio.prelude.NonEmptyList

object SafeRaceFirstTestApp extends zio.ZIOAppDefault {
  override def run: RIO[Environment with ZIOAppArgs with Scope, ExitCode] = {
    val app = Race
      .apply(
        NonEmptyList.fromIterable(makeEffect(0), (1 to 2).map(makeEffect))
      )
      .run()
      .interruptibleRace(
        ZIO
          .acquireRelease(ZIO.succeed(println("acquire"))) { _ =>
            ZIO.succeed(println("release"))
          }
          .zipRight {
            ZIO
              .suspend(ZIO.succeed(println("In use")))
              .repeat(Schedule.fixed(Duration.fromSeconds(1L)).whileOutput(_ < 3))
              .onExit(e => ZIO.succeed(println(s"Use exit $e")))
          }
          .unit
      )
    app
      .as(ExitCode(0))
  }

  private def makeEffect(i: Int) =
    ZIO
      .suspend(ZIO.succeed(println(s"Par $i")))
      .repeat(Schedule.fixed(Duration.fromSeconds(1L)).whileOutput(_ < 5))
      .unit
      .onInterrupt(_ =>
        ZIO.succeed(println(s"Par $i interrupting")) *>
          ZIO.succeed(println(s"Par $i interrupted")).delay(Duration.fromSeconds(3L))
      )
}
