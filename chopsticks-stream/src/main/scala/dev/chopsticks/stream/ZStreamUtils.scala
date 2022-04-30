package dev.chopsticks.stream

import zio.Schedule.Decision
import zio.{Chunk, Schedule, ZIO}
import zio.clock.Clock
import zio.stream.ZStream

import java.time.Duration

object ZStreamUtils {

  final case class RetryState(nextDelay: Duration, count: Long, elapsed: Duration, willContinue: Boolean)
  final case class FailedAttempt[E](error: E, state: RetryState)

  def retry[R, E, V](
    effect: ZIO[R, E, V],
    retrySchedule: Schedule[Clock, Any, Duration]
  ): ZStream[Clock with R, Nothing, Either[FailedAttempt[E], V]] = {
    val schedule: Schedule[Clock, E, ((Duration, Long), Duration)] =
      Schedule.elapsed && Schedule.count && retrySchedule

    for {
      leftQueue <- ZStream.fromEffect(zio.Queue.unbounded[Either[E, V]])
      rightQueue <- ZStream.fromEffect(zio.Queue.unbounded[Either[RetryState, V]])
      ret <- {
        ZStream
          .fromEffect(
            effect
              .retry(schedule.tapInput((e: E) => leftQueue.offer(Left(e))).onDecision {
                case Decision.Done(((elapsed, count), nextDelay)) =>
                  rightQueue.offer(Left(RetryState(nextDelay, count, elapsed, willContinue = false)))
                case Decision.Continue(((elapsed, count), nextDelay), _, _) =>
                  rightQueue.offer(Left(RetryState(nextDelay, count, elapsed, willContinue = true)))
              })
              .either
              .flatMap {
                case Left(_) => ZIO.succeed(Chunk.empty)
                case Right(result) =>
                  leftQueue.offer(Right(result)) *> rightQueue.offer(Right(result))
                    .as(Chunk.empty)
              }
          )
          .merge(
            ZStream
              .fromQueue(leftQueue)
              .zip(ZStream.fromQueue(rightQueue))
              .map {
                case (Left(error), Left(retryState)) =>
                  Chunk.single(Left(FailedAttempt(
                    error = error,
                    state = retryState
                  )))

                case (Right(v), Right(_)) =>
                  Chunk.single(Right(v))

                case _ =>
                  Chunk.empty
              }
          )
          .mapConcatChunk(identity)
          .takeUntil {
            case Left(failure) => !failure.state.willContinue
            case Right(_) => true
          }
      }
    } yield ret
  }
}
