package dev.chopsticks.stream

import zio.Schedule.Decision
import zio.{Chunk, Schedule, UIO, ZIO}
import zio.stream.ZStream

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn

object ZStreamUtils {

  final case class RetryState(nextDelay: Duration, count: Long, elapsed: Duration, willContinue: Boolean)
  final case class FailedAttempt[E](error: E, state: RetryState)

  sealed abstract private class ResultQueueItem[+E, +V] extends Product with Serializable
  private object ResultQueueItem {
    final case class Failure[E](error: E) extends ResultQueueItem[E, Nothing]
    final case class Success[V](value: V) extends ResultQueueItem[Nothing, V]
    final case object Interrupted extends ResultQueueItem[Nothing, Nothing]
  }

  sealed abstract private class StateQueueItem[+V] extends Product with Serializable
  private object StateQueueItem {
    final case class Failure(retryState: RetryState) extends StateQueueItem[Nothing]
    final case class Success[V](value: V) extends StateQueueItem[V]
    final case object Interrupted extends StateQueueItem[Nothing]
  }

  sealed abstract private class Result[+E, +V] extends Product with Serializable
  private object Result {
    final case class Outcome[E, V](value: Either[E, V]) extends Result[E, V]
    final case object Interrupted extends Result[Nothing, Nothing]
  }

  final private case class State(
    // effect may be either scheduled or running
    isRunning: Boolean,
    interrupted: Boolean
  )

  @nowarn("cat=lint-infer-any")
  def retry[R, E, V](
    effect: ZIO[R, E, V],
    retrySchedule: Schedule[Any, Any, Duration],
    completionSignal: UIO[Unit]
  ): ZStream[R, Nothing, Either[FailedAttempt[E], V]] = {
    val schedule = Schedule.elapsed && Schedule.count && retrySchedule

    for {
      leftQueue <- ZStream.fromZIO(zio.Queue.unbounded[ResultQueueItem[E, V]])
      rightQueue <- ZStream.fromZIO(zio.Queue.unbounded[StateQueueItem[V]])
      streamState = new AtomicReference[State](State(isRunning = false, interrupted = false))
      enqueueInterruption = {
        val io: UIO[Unit] = (leftQueue.offer(ResultQueueItem.Interrupted) *>
          rightQueue.offer(StateQueueItem.Interrupted)).unit
        io
      }
      // handle interruption
      fib <- ZStream.fromZIO {
        completionSignal
          .zipRight {
            for {
              updatedState <- ZIO.succeed(streamState.updateAndGet(_.copy(interrupted = true)))
              // if the effect is running, then it will handle graceful interruption
              _ <- enqueueInterruption.unless(updatedState.isRunning)
            } yield ()
          }
          .interruptAllChildren
          .fork
      }
      ret <- {
        ZStream
          .fromZIO(
            {
              val io = {
                for {
                  completionState <- ZIO.succeed {
                    streamState.updateAndGet { state =>
                      if (state.interrupted) state
                      else state.copy(isRunning = true)
                    }
                  }
                  _ <- enqueueInterruption.when(completionState.interrupted)
                  res <- if (completionState.interrupted) ZIO.never else effect
                } yield res
              }
              io
                .retry {
                  schedule.tapInput((e: E) => leftQueue.offer(ResultQueueItem.Failure(e))).onDecision {
                    case (_, (elapsed, count, nextDelay), decision) =>
                      decision match {
                        case Decision.Done =>
                          // no need to enqueue interruption here, because the stream is done after this
                          rightQueue.offer(StateQueueItem.Failure(RetryState(
                            nextDelay,
                            count,
                            elapsed,
                            willContinue = false
                          )))
                        case Decision.Continue(_) =>
                          rightQueue.offer(StateQueueItem.Failure(RetryState(
                            nextDelay,
                            count,
                            elapsed,
                            willContinue = true
                          ))) <* {
                            val state = streamState.updateAndGet(_.copy(isRunning = false))
                            enqueueInterruption.when(state.interrupted)
                          }
                      }
                  }
                }
                .either
                .flatMap {
                  case Left(_) =>
                    val state = streamState.updateAndGet(_.copy(isRunning = false))
                    enqueueInterruption.when(state.interrupted).as(Chunk.empty)
                  case Right(result) =>
                    // no need to enqueue interruption here, because the stream is done after this
                    leftQueue.offer(ResultQueueItem.Success(result)) *>
                      rightQueue.offer(StateQueueItem.Success(result))
                        .as(Chunk.empty)
                }
                .as(Chunk.empty)
            }
          )
          .merge(
            ZStream
              .fromQueue(leftQueue)
              .zip(ZStream.fromQueue(rightQueue))
              .map {
                case (ResultQueueItem.Failure(error), StateQueueItem.Failure(retryState)) =>
                  Chunk.single(Result.Outcome(Left(FailedAttempt(
                    error = error,
                    state = retryState
                  ))))

                case (ResultQueueItem.Success(v), StateQueueItem.Success(_)) =>
                  Chunk.single(Result.Outcome(Right(v)))

                case (ResultQueueItem.Interrupted, StateQueueItem.Interrupted) =>
                  Chunk.single(Result.Interrupted)

                case _ =>
                  Chunk.empty
              }
          )
          .mapConcatChunk(identity)
          .takeUntil {
            case Result.Outcome(Left(failure)) => !failure.state.willContinue
            case Result.Outcome(Right(_)) => true
            case Result.Interrupted => true
          }
          .collect {
            case Result.Outcome(result) => result
          }
          .ensuring(fib.interrupt)
      }
    } yield ret
  }
}
