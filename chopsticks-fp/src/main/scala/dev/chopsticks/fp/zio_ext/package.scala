package dev.chopsticks.fp

import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.util.implicits.SquantsImplicits._
import logstage.Log
import squants.time.Nanoseconds
import zio._
import zio.duration._
import zio.clock.Clock

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.implicitConversions

package object zio_ext {
  private val nanoTime = ZIO.accessM[Clock](_.get.nanoTime)

  implicit def scalaToZioDuration(d: Duration): zio.duration.Duration =
    zio.duration.Duration.fromScala(d)

  type MeasuredLogging = IzLogging with Clock

  implicit final class TaskExtensions[R >: Nothing, E <: Throwable, A](io: ZIO[R, E, A]) {
    def unsafeRunToFuture(implicit rt: Runtime[R]): Future[A] = {
      val promise = scala.concurrent.Promise[A]()
      rt.unsafeRunAsync(io) {
        case Exit.Success(value) => val _ = promise.success(value)
        case Exit.Failure(cause) => promise.failure(cause.squashTrace)
      }
      promise.future
    }
  }

  implicit final class ZIOExtensions[R >: Nothing, E <: Any, A](io: ZIO[R, E, A]) {
    def interruptAllChildrenPar: ZIO[R, E, A] = {
      io.ensuringChildren(fibs => {
        ZIO.fiberId.flatMap { fs =>
          ZIO.foreachPar_(fibs)(_.interruptAs(fs))
        }
      })
    }

    def debugResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      logResult(name, result, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

    def logResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      bracketStartMeasurement(name, result, logTraceOnError) { startTime =>
        measurementHandleInterruption(name, startTime)(io)
      }
    }

    def log(name: String, logTraceOnError: Boolean = true)(implicit ctx: LogCtx): ZIO[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "completed", logTraceOnError)
    }

    def logPeriodically(name: String, interval: FiniteDuration, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      val renderResult: A => String = _ => "completed"
      bracketStartMeasurement(name, renderResult, logTraceOnError) { startTime =>
        val logTask = for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
          logger <-
            ZIO.access[IzLogging](_.get.zioLoggerWithCtx(ctx).withCustomContext("task" -> name, "elapsed" -> elapsed))
          _ = logger.log(ctx.level)(s"waiting for completion")
        } yield ()
        measurementHandleInterruption(name, startTime) {
          for {
            fib <- logTask.repeat(Schedule.fixed(interval)).delay(interval).fork
            result <- io.onExit(_ => fib.interrupt)
          } yield result
        }
      }
    }

    def debug(name: String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      log(name, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

    def retryForever[R1 <: R](
      retryPolicy: Schedule[R1, Any, Any],
      repeatSchedule: Schedule[R1, Any, Any],
      retryResetMinDuration: zio.duration.Duration
    ): ZIO[R1 with Clock, Nothing, Unit] = {
      io.either.timed
        .flatMap {
          case (elapsed, result) =>
            result match {
              case Right(_) => ZIO.succeed(())
              case Left(_) if elapsed > retryResetMinDuration => ZIO.succeed(())
              case Left(ex) => ZIO.fail(ex)
            }
        }
        .retry(retryPolicy)
        .repeat(repeatSchedule.unit)
        .orDieWith(_ => new IllegalStateException("Can't happen with infinite retries"))
    }

    private def bracketStartMeasurement(name: String, renderResult: A => String, logTraceOnError: Boolean) = {
      ZIO.bracketExit(startMeasurement(name, "started"))((startTime: Long, exit: Exit[E, A]) =>
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
      io.onInterrupt {
        for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
          _ <-
            ZIO.access[IzLogging](_
              .get
              .loggerWithCtx(ctx)
              .withCustomContext("task" -> name, "elapsed" -> elapsed)
              .log(ctx.level)("interrupting..."))
        } yield ()
      }
    }
  }

  implicit final class ZManagedExtensions[R >: Nothing, E <: Any, A](managed: ZManaged[R, E, A]) {
    def logResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      for {
        startTimeRef <- Ref.make[Long](0).toManaged_
        value <- managed
          .flatMap { value =>
            for {
              startTime <- ZManaged.fromEffect(startMeasurement(name, result(value)))
              _ <- startTimeRef.set(startTime).toManaged_
            } yield value
          }
          .onExit { exit =>
            for {
              startTime <- startTimeRef.get
              _ <- logElapsedTime(
                name = name,
                startTimeNanos = startTime,
                exit = exit,
                renderResult = (_: A) => "completed",
                logTraceOnError = logTraceOnError
              )
            } yield ()
          }
      } yield value
    }

    def debugResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      logResult(name, result, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

    def log(name: String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "started", logTraceOnError)
    }

    def debug(name: String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      log(name, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    }

  }

  private def startMeasurement(name: String, message: String)(implicit ctx: LogCtx): URIO[MeasuredLogging, Long] = {
    for {
      time <- nanoTime
      _ <- ZIO.access[IzLogging](_.get.loggerWithCtx(ctx).withCustomContext("task" -> name).log(ctx.level)(message))
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
      logger <-
        ZIO.access[IzLogging](_.get.zioLoggerWithCtx(ctx).withCustomContext("task" -> name, "elapsed" -> elapsed))
      _ <- exit.toEither match {
        case Left(FiberFailure(cause)) if cause.interrupted =>
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
    } yield ()
  }
}
