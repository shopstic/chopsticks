package dev.chopsticks.fp.util

import dev.chopsticks.fp.iz_logging.LogCtx
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, ZIOExtensions}
import zio.{RIO, Task, ZIO}

import scala.collection.immutable.Queue

final class LoggedRace[-R] private (queue: Queue[R with MeasuredLogging => Task[Unit]]) {
  def add[R1 <: R](name: String, task: RIO[R1, Unit])(implicit logCtx: LogCtx): LoggedRace[R1] = {
    new LoggedRace[R1](queue.enqueue((env: R1 with MeasuredLogging) => task.log(name).provide(env)))
  }

  def run(): RIO[R with MeasuredLogging, Unit] = {
    ZIO
      .environment[R with MeasuredLogging]
      .flatMap { env =>
        val tasks = queue.toList.map(_(env))

        tasks match {
          case head :: tail :: Nil =>
            head.raceFirst(tail)
          case head :: tail =>
            head.run.raceAll(tail.map(_.run)).flatMap(ZIO.done(_)).refailWithTrace
          case Nil =>
            Task.unit
        }
      }
      .interruptAllChildrenPar
  }
}

object LoggedRace {
  def apply() = new LoggedRace[Any](Queue.empty)
  def apply[R](seq: Iterable[(String, RIO[R, Unit])])(implicit logCtx: LogCtx): LoggedRace[R] = {
    seq
      .foldLeft(new LoggedRace[R](Queue.empty)) { case (race, (name, task)) =>
        race.add(name, task)
      }
  }
}
