package dev.chopsticks.fp.util

import cats.data.NonEmptyList
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import zio.clock.Clock
import zio.{IO, ZIO}

object LoggedRace {
  final class EmptyLoggedRace[-R, +E, +A] private[util] {
    def add[R1 <: R, E1 >: E, A1 >: A](name: String, io: ZIO[R1, E1, A1])(implicit
      logCtx: LogCtx
    ): NonEmptyLoggedRace[R1, E1, A1] = {
      new NonEmptyLoggedRace[R1, E1, A1](NonEmptyList.one((env: R1 with IzLogging with Clock) =>
        io.log(name).provide(env)
      ))
    }
  }

  final class NonEmptyLoggedRace[-R, +E, +A] private[util] (
    queue: NonEmptyList[R with IzLogging with Clock => IO[E, A]]
  ) {
    def add[R1 <: R, E1 >: E, A1 >: A](name: String, io: ZIO[R1, E1, A1])(implicit
      logCtx: LogCtx
    ): NonEmptyLoggedRace[R1, E1, A1] = {
      new NonEmptyLoggedRace[R1, E1, A1](((env: R1 with IzLogging with Clock) => io.log(name).provide(env)) :: queue)
    }

    def run(): ZIO[R with IzLogging with Clock, E, A] = {
      ZIO
        .environment[R with IzLogging with Clock]
        .flatMap { env =>
          ZIO
            .bracket(ZIO.foreach(queue.toList)(fn => fn(env).interruptible.fork)) { fibers =>
              ZIO.foreachPar_(fibers)(_.interrupt)
            } {
              case head :: tail :: Nil =>
                head.join.raceFirst(tail.join)
              case head :: tail =>
                head.join.either.raceAll(tail.map(_.join.either))
                  .flatMap(ZIO.fromEither(_))
              case Nil => ??? // Impossible at the type-level
            }
        }
    }
  }

  def apply[A]() = new EmptyLoggedRace[Any, Nothing, A]
  def apply[R, E, A](head: (String, ZIO[R, E, A]), tail: (String, ZIO[R, E, A])*): NonEmptyLoggedRace[R, E, A] = {
    tail
      .foldLeft(
        apply[A]().add(head._1, head._2)
      ) { case (race, (name, effect)) =>
        race.add(name, effect)
      }
  }
}
