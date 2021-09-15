package dev.chopsticks.fp.util

import cats.data.NonEmptyList
import zio.{IO, ZIO}

object Race {
  final class EmptyRace[-R, +E, +A]() {
    def add[R1 <: R, E1 >: E, A1 >: A](head: ZIO[R1, E1, A1], tail: ZIO[R1, E1, A1]*): NonEmptyRace[R1, E1, A1] = {
      tail
        .foldLeft(
          new NonEmptyRace[R1, E1, A1](NonEmptyList.one((env: R1) => head.provide(env)))
        ) { (race, next) =>
          race.add(next)
        }
    }
  }

  final class NonEmptyRace[-R, +E, +A] private[util] (queue: NonEmptyList[R => IO[E, A]]) {
    def add[R1 <: R, E1 >: E, A1 >: A](
      head: ZIO[R1, E1, A1],
      tail: ZIO[R1, E1, A1]*
    ): NonEmptyRace[R1, E1, A1] = {
      tail
        .foldLeft(
          new NonEmptyRace[R1, E1, A1](queue.prepend((env: R1) => head.provide(env)))
        ) { (race, next) =>
          race.add(next)
        }
    }

    def run(): ZIO[R, E, A] = {
      ZIO
        .environment[R]
        .flatMap { env =>
          ZIO
            .bracket(ZIO.foreach(queue.toList)(fn => fn(env).interruptible.fork)) { fibers =>
              ZIO.foreachPar_(fibers)(_.interrupt)
            } {
              case head :: tail :: Nil =>
                head.join.raceFirst(tail.join)
              case head :: tail =>
                head.join.raceAll(tail.map(_.join))
              case Nil => ??? // Impossible at the type-level
            }
        }
    }
  }

  def apply[A]() = new EmptyRace[Any, Nothing, A]
  def apply[R, E, A](head: ZIO[R, E, A], tail: ZIO[R, E, A]*): NonEmptyRace[R, E, A] = {
    apply[A]().add(head, tail: _*)
  }

  def apply[R, E, A](list: NonEmptyList[ZIO[R, E, A]]): NonEmptyRace[R, E, A] = {
    apply[A]().add(list.head, list.tail: _*)
  }
}
