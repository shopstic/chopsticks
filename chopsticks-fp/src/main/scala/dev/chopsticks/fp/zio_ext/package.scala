package dev.chopsticks.fp

import dev.chopsticks.fp.log_env.{LogCtx, LogEnv}
import dev.chopsticks.util.implicits.SquantsImplicits._
import squants.time.Nanoseconds
import zio._
import zio.clock.Clock

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

package object zio_ext {
  private val nanoTime = ZIO.accessM[Clock](_.get.nanoTime)

  implicit def scalaToZioDuration(d: Duration): zio.duration.Duration =
    zio.duration.Duration.fromScala(d)

  type MeasuredLogging = LogEnv with Clock

  implicit final class ZIOExtensions[R >: Nothing, E <: Any, A](io: ZIO[R, E, A]) {
    def logResult(name: String, result: A => String)(
      implicit ctx: LogCtx
    ): ZIO[R with MeasuredLogging, E, A] = {
      val start: ZIO[R with MeasuredLogging, E, Long] = nanoTime <* ZLogger.info(s"[$name] started")

      ZIO.bracketExit(start) { (startTime: Long, exit: Exit[E, A]) =>
        for {
          elapse <- nanoTime.map(endTime => endTime - startTime)
          formattedElapse = Nanoseconds(elapse).inBestUnit.rounded(2)
          _ <- exit.toEither match {
            case Left(FiberFailure(cause)) if cause.interrupted =>
              ZLogger.warn(s"[$name] [took $formattedElapse] interrupted")

            case Left(e) =>
              ZLogger.error(s"[$name] [took $formattedElapse] failed", e)

            case Right(r) =>
              ZLogger.info(s"[$name] [took $formattedElapse] ${result(r)}")
          }
        } yield ()
      } { startTime =>
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
}
