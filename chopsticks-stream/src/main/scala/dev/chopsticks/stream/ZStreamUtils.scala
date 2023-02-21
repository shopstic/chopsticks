package dev.chopsticks.stream

import zio.Schedule.Decision
import zio.{Chunk, Schedule, ZIO}
import zio.stream.ZStream

import java.time.Duration

object ZStreamUtils {

  final case class RetryState(nextDelay: Duration, count: Long, elapsed: Duration, willContinue: Boolean)
  final case class FailedAttempt[E](error: E, state: RetryState)

  def retry[R, E, V](
    effect: ZIO[R, E, V],
    retrySchedule: Schedule[Any, Any, Duration]
  ): ZStream[R, Nothing, Either[FailedAttempt[E], V]] = {
    val schedule =
      Schedule.elapsed && Schedule.count && retrySchedule

    for {
      leftQueue <- ZStream.fromZIO(zio.Queue.unbounded[Either[E, V]])
      rightQueue <- ZStream.fromZIO(zio.Queue.unbounded[Either[RetryState, V]])
      ret <- {
        ZStream
          .fromZIO(
            effect
              .retry(schedule.tapInput((e: E) => leftQueue.offer(Left(e))).onDecision {
                case (_, (elapsed, count, nextDelay), decision) =>
                  val willContinue =
                    decision match
                      case Decision.Done => false
                      case Decision.Continue(_) => true
                  rightQueue.offer(Left(RetryState(nextDelay, count, elapsed, willContinue)))
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
