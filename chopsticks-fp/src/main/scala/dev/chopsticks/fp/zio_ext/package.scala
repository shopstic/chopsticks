package dev.chopsticks.fp

import dev.chopsticks.fp.log_env.{LogCtx, LogEnv}
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

  type MeasuredLogging = LogEnv with Clock

  implicit final class TaskExtensions[R >: Nothing, E <: Throwable, A](io: ZIO[R, E, A]) {
    def unsafeRunToFuture(implicit rt: Runtime[R]): Future[A] = {
      val promise = scala.concurrent.Promise[A]()
      rt.unsafeRunAsync(io) {
        case Exit.Success(value) => promise.success(value)
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

    def logResult(name: String, result: A => String)(implicit ctx: LogCtx): ZIO[R with MeasuredLogging, E, A] = {
      ZIO.bracketExit(startMeasurement(name))((startTime: Long, exit: Exit[E, A]) =>
        logElapsedTime(name, startTime, exit, result)
      ) { startTime =>
        io.onInterrupt(for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          formattedElapse = Nanoseconds(elapse).inBestUnit.rounded(2)
          _ <- ZLogger.info(s"[$name] [elapsed $formattedElapse] interrupting...")
        } yield ())
      }
    }

    def log(name: String)(implicit ctx: LogCtx): ZIO[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "completed")
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
    def logResult(name: String, result: A => String)(implicit
      ctx: LogCtx
    ): ZManaged[R with MeasuredLogging, E, A] = {
      for {
        startTime <- ZManaged.fromEffect(startMeasurement(name))
        result <- managed.onExit(exit => logElapsedTime(name, startTime, exit, result))
      } yield result
    }

    def log(name: String)(implicit ctx: LogCtx): ZManaged[R with MeasuredLogging, E, A] = {
      logResult(name, _ => "completed")
    }
  }

  private def startMeasurement(name: String)(implicit ctx: LogCtx): URIO[MeasuredLogging, Long] =
    nanoTime <* ZLogger.info(s"[$name] started")

  private def logElapsedTime[E, A](
    name: String,
    startTimeNanos: Long,
    exit: Exit[E, A],
    renderResult: A => String
  )(implicit logCtx: LogCtx) = {
    for {
      elapse <- nanoTime.map(endTime => endTime - startTimeNanos)
      formattedElapse = Nanoseconds(elapse).inBestUnit.rounded(2)
      _ <- exit.toEither match {
        case Left(FiberFailure(cause)) if cause.interrupted =>
          ZLogger.warn(s"[$name] [took $formattedElapse] interrupted")

        case Left(e) =>
          ZLogger.error(s"[$name] [took $formattedElapse] failed", e)

        case Right(r) =>
          ZLogger.info(s"[$name] [took $formattedElapse] ${renderResult(r)}")
      }
    } yield ()
  }
}
