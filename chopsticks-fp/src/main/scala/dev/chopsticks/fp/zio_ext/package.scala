package dev.chopsticks.fp

import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.util.Race
import dev.chopsticks.util.implicits.SquantsImplicits._
import logstage.Log
import squants.time.Nanoseconds
import zio._

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.implicitConversions

package object zio_ext {
  private val nanoTime = ZIO.clock.flatMap(_.nanoTime)

  implicit def scalaToZioDuration(d: Duration): zio.Duration =
    zio.Duration.fromScala(d)

  type MeasuredLogging = IzLogging

  implicit final class TaskExtensions[R >: Nothing, E <: Throwable, A](io: ZIO[R, E, A]) {
    def unsafeRunToFuture(implicit rt: Runtime[R]): Future[A] = {
      val promise = scala.concurrent.Promise[A]()
      Unsafe.unsafe { implicit unsafe =>
        rt.unsafe
          .fork(io)
          .unsafe
          .addObserver {
            case Exit.Success(value) => val _ = promise.success(value)
            case Exit.Failure(cause) => val _ = promise.failure(cause.squashTrace)
          }
      }
      promise.future
    }
  }

  implicit final class ZIOExtensions[R >: Nothing, E <: Any, A](io: ZIO[R, E, A]) {
    def interruptibleRace[R1 <: R, E1 >: E, A1 >: A](that: ZIO[R1, E1, A1]): ZIO[R1, E1, A1] = {
      Race(io).add(that).run()
    }

    def interruptAllChildrenPar: ZIO[R, E, A] = {
      io.ensuringChildren(fibs => {
        ZIO.fiberId.flatMap { fs =>
          ZIO.foreachParDiscard(fibs)(_.interruptAs(fs))
        }
      })
    }

    def debugResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
      ctx: LogCtx
    ): ZIO[R with IzLogging, E, A] = {
      logResult(name, result, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

    def logResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
      ctx: LogCtx
    ): ZIO[R with IzLogging, E, A] = {
      bracketStartMeasurement(name, result, logTraceOnError)(ctx) { startTime =>
        measurementHandleInterruption(name, startTime)(io)
      }
    }

    def log(name: String, logTraceOnError: Boolean = false)(implicit
      ctx: LogCtx
    ): ZIO[R with IzLogging, E, A] = {
      logResult(name, _ => "completed", logTraceOnError)
    }

    def logPeriodically(name: String, interval: FiniteDuration, logTraceOnError: Boolean = false)(implicit
      ctx: LogCtx
    ): ZIO[R with IzLogging, E, A] = {
      val renderResult: A => String = _ => "completed"
      bracketStartMeasurement(name, renderResult, logTraceOnError)(ctx) { startTime =>
        val logTask = for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
          _ <- IzLogging.loggerWithContext(ctx).map {
            _
              .withCustomContext("task" -> name, "elapsed" -> elapsed)
              .log(ctx.level)(s"waiting for completion")
          }
        } yield ()
        measurementHandleInterruption(name, startTime) {
          for {
            fib <- logTask.repeat(Schedule.fixed(interval)).delay(interval).fork
            result <- io.onExit(_ => fib.interrupt)
          } yield result
        }
      }
    }

    def debug(name: String, logTraceOnError: Boolean = false)(implicit
      ctx: LogCtx
    ): ZIO[R with IzLogging, E, A] = {
      log(name, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

    def retryForever[R1 <: R](
      retryPolicy: Schedule[R1, Any, Any],
      repeatSchedule: Schedule[R1, Any, Any],
      retryResetMinDuration: zio.Duration
    ): ZIO[R1, Nothing, Unit] = {
      io.either.timed
        .flatMap {
          case (elapsed, result) =>
            result match {
              case Right(_) => ZIO.unit
              case Left(_) if elapsed > retryResetMinDuration => ZIO.unit
              case Left(ex) => ZIO.fail(ex)
            }
        }
        .retry(retryPolicy)
        .repeat(repeatSchedule.unit)
        .orDieWith(_ => new IllegalStateException("Can't happen with infinite retries"))
    }

    private def bracketStartMeasurement(name: String, renderResult: A => String, logTraceOnError: Boolean)(implicit
      ctx: LogCtx
    ) = {
      ZIO.acquireReleaseExitWith(startMeasurement(name, "started"))((startTime: Long, exit: Exit[E, A]) =>
        logElapsedTime(
          name = name,
          startTimeNanos = startTime,
          exit = exit,
          renderResult = renderResult,
          logTraceOnError = logTraceOnError
        )
      )
    }

    private def measurementHandleInterruption[R1, E2, A2](name: String, startTime: Long)(io: ZIO[R1, E2, A2])(implicit
      ctx: LogCtx
    ) = {
      ZIO
        .acquireReleaseExitWith(io.interruptible.fork) { (fib, exit: Exit[Any, Any]) =>
          val log = for {
            elapse <- nanoTime.map(endTime => endTime - startTime)
            elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
            _ <- IzLogging.loggerWithContext(ctx).map {
              _
                .withCustomContext("task" -> name, "elapsed" -> elapsed)
                .log(ctx.level)("interrupting...")
            }
          } yield ()

          log.when(exit.isInterrupted) *> fib.interrupt
        } { fib =>
          fib.join
        }
    }
  }

  private def startMeasurement(name: String, message: String)(implicit
    ctx: LogCtx
  ): URIO[IzLogging, Long] = {
    for {
      time <- nanoTime
      _ <- IzLogging.loggerWithContext(ctx).map(_.withCustomContext("task" -> name).log(ctx.level)(message))
    } yield time
  }

  private def logElapsedTime[E, A](
    name: String,
    startTimeNanos: Long,
    exit: Exit[E, A],
    renderResult: A => String,
    logTraceOnError: Boolean
  )(implicit ctx: LogCtx) = {
    for {
      elapse <- nanoTime.map(endTime => endTime - startTimeNanos)
      elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
      errorLevel = if (ctx.level.compareTo(Log.Level.Info) >= 0) Log.Level.Error else ctx.level
      logger <- IzLogging.loggerWithContext(ctx).map(_.withCustomContext("task" -> name, "elapsed" -> elapsed))
      _ <- ZIO.succeed {
        exit.toEither match {
          case Left(FiberFailure(cause)) if cause.isInterrupted =>
            logger.log(ctx.level)(s"interrupted")

          case Left(exception @ FiberFailure(cause)) =>
            if (logTraceOnError) {
              logger.log(errorLevel)(s"failed $exception")
            }
            else {
              logger.log(errorLevel)(s"failed ${cause.untraced -> "cause"}")
            }

          case Left(exception) =>
            if (logTraceOnError) {
              logger.log(errorLevel)(s"failed $exception")
            }
            else {
              logger.log(errorLevel)(s"failed ${exception.getMessage -> "message"}")
            }

          case Right(r) =>
            val renderedResult = renderResult(r)
            logger.log(ctx.level)(s"${renderedResult -> "" -> null}")
        }
      }
    } yield ()
  }
}
