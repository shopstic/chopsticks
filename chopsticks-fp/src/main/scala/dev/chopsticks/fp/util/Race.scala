package dev.chopsticks.fp.util

import zio.{IO, Trace, ZEnvironment, ZIO}
import zio.prelude.NonEmptyList

object Race:
  final class EmptyRace[-R, +E, +A]() {
    def add[R1 <: R, E1 >: E, A1 >: A](head: ZIO[R1, E1, A1], tail: ZIO[R1, E1, A1]*)(using
      trace: Trace
    ): NonEmptyRace[R1, E1, A1] =
      tail
        .foldLeft(
          new NonEmptyRace[R1, E1, A1](NonEmptyList.single(env => head.provideEnvironment(env)(trace)))
        ) { (race, next) =>
          race.add(next)
        }
  }

  final class NonEmptyRace[-R, +E, +A] private[util] (queue: NonEmptyList[ZEnvironment[R] => IO[E, A]]) {
    def add[R1 <: R, E1 >: E, A1 >: A](
      head: ZIO[R1, E1, A1],
      tail: ZIO[R1, E1, A1]*
    )(using trace: Trace): NonEmptyRace[R1, E1, A1] = {
      tail
        .foldLeft(
          new NonEmptyRace[R1, E1, A1](
            NonEmptyList.cons((env: ZEnvironment[R1]) => head.provideEnvironment(env)(trace), queue)
          )
        ) { (race, next) =>
          race.add(next)
        }
    }

    def run(): ZIO[R, E, A] =
      ZIO
        .environment[R]
        .flatMap { env =>
          ZIO
            .acquireReleaseWith(ZIO.foreach(queue.toList)(fn => fn(env).interruptible.fork)) { fibers =>
              ZIO.foreachParDiscard(fibers)(_.interrupt)
            } {
              case head :: tail :: Nil =>
                head.join.raceFirst(tail.join)
              case head :: tail =>
                head.join.either.raceAll(tail.map(_.join.either))
                  .flatMap(ZIO.fromEither(_))
              case Nil =>
                ??? // Impossible at the type-level
            }
        }
  }

  def apply[A]() =
    new EmptyRace[Any, Nothing, A]

  def apply[R, E, A](head: ZIO[R, E, A], tail: ZIO[R, E, A]*): NonEmptyRace[R, E, A] =
    apply[A]().add(head, tail: _*)

  def apply[R, E, A](list: NonEmptyList[ZIO[R, E, A]]): NonEmptyRace[R, E, A] =
    apply[A]().add(list.head, list.tail: _*)
