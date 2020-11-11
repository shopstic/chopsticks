package dev.chopsticks.fp

import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.util.implicits.SquantsImplicits._
import squants.time.Nanoseconds
import zio._
import zio.duration._
import zio.clock.Clock

import scala.concurrent.Future
import scala.concurrent.duration.Duration
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

    def logResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      ZIO.bracketExit(startMeasurement(name))((startTime: Long, exit: Exit[E, A]) =>
        logElapsedTime(
          name = name,
          startTimeNanos = startTime,
          exit = exit,
          renderResult = result,
          logTraceOnError = logTraceOnError
        )
      ) { startTime =>
        io.onInterrupt(for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
          _ <-
            ZIO.access[IzLogging](_
              .get
              .loggerWithCtx(ctx)
              .withCustomContext("task" -> name, "elapsed" -> elapsed)
              .info("interrupting..."))
        } yield ())
      }
    }

    def log(name: String, logTraceOnError: Boolean = true)(implicit ctx: LogCtx): ZIO[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "completed", logTraceOnError)
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
  }

  implicit final class ZManagedExtensions[R >: Nothing, E <: Any, A](managed: ZManaged[R, E, A]) {
    def logResult(name: String, result: A => String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      for {
        startTime <- ZManaged.fromEffect(startMeasurement(name))
        result <- managed.onExit(exit =>
          logElapsedTime(
            name = name,
            startTimeNanos = startTime,
            exit = exit,
            renderResult = result,
            logTraceOnError = logTraceOnError
          )
        )
      } yield result
    }

    def log(name: String, logTraceOnError: Boolean = true)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "completed", logTraceOnError)
    }
  }

  private def startMeasurement(name: String)(implicit ctx: LogCtx): URIO[MeasuredLogging, Long] = {
    for {
      time <- nanoTime
      _ <- ZIO.access[IzLogging](_.get.loggerWithCtx(ctx).withCustomContext("task" -> name).info("started"))
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
      logger <-
        ZIO.access[IzLogging](_.get.zioLoggerWithCtx(ctx).withCustomContext("task" -> name, "elapsed" -> elapsed))
      _ <- exit.toEither match {
        case Left(FiberFailure(cause)) if cause.interrupted =>
          logger.warn(s"interrupted")

        case Left(exception @ FiberFailure(cause)) =>
          if (logTraceOnError) {
            logger.error(s"failed $exception")
          }
          else {
            logger.error(s"failed ${cause.untraced -> "cause"}")
          }

        case Left(exception) =>
          if (logTraceOnError) {
            logger.error(s"failed $exception")
          }
          else {
            logger.error(s"failed ${exception.getMessage -> "message"}")
          }

        case Right(r) =>
          val renderedResult = renderResult(r)
          logger.info(s"${renderedResult -> "" -> null}")
      }
    } yield ()
  }
}
