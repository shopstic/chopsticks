package dev.chopsticks.fp

import dev.chopsticks.fp.util.Race
import dev.chopsticks.util.implicits.SquantsImplicits._
import squants.time.Nanoseconds
import zio.*

import scala.language.implicitConversions

package object zio_ext:

  private[zio_ext] val nanoTime = ZIO.clock.flatMap(_.nanoTime)

  // extension [R >: Nothing, E <: Throwable, A](io: ZIO[R, E, A]):
  //   def unsafeRunToFuture: Future[A] =
  //     Unsafe.unsafe {}
  // val promise = scala.concurrent.Promise[A]()
  // rt.unsafeRunToFuture(io) {
  //   case Exit.Success(value) => val _ = promise.success(value)
  //   case Exit.Failure(cause) => promise.failure(cause.squashTrace)
  // }
  // promise.future

  extension [R >: Nothing, E <: Any, A](io: ZIO[R, E, A])
    def interruptibleRace[R1 <: R, E1 >: E, A1 >: A](that: ZIO[R1, E1, A1]): ZIO[R1, E1, A1] =
      Race(io).add(that).run()

    def interruptAllChildrenPar: ZIO[R, E, A] =
      io.ensuringChildren(fibs => {
        ZIO.fiberId.flatMap { fs =>
          ZIO.foreachParDiscard(fibs)(_.interruptAs(fs))
        }
      })

    // def debugResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
    //   ctx: LogCtx
    // ): ZIO[R, E, A] = {
    //   logResult(name, result, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    // }

    def logResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
      trace: Trace
    ): ZIO[R, E, A] =
      bracketStartMeasurement(name, result, logTraceOnError)(trace) { startTime =>
        measurementHandleInterruption(name, startTime)(io)
      }

    def log(name: String, logTraceOnError: Boolean = false)(implicit trace: Trace): ZIO[R, E, A] =
      logResult(name, _ => "completed", logTraceOnError)

    // def logPeriodically(name: String, interval: FiniteDuration, logTraceOnError: Boolean = false)(implicit
    //   ctx: LogCtx
    // ): ZIO[R with IzLogging with Clock, E, A] = {
    //   val renderResult: A => String = _ => "completed"
    //   bracketStartMeasurement(name, renderResult, logTraceOnError)(ctx) { startTime =>
    //     val logTask = for {
    //       elapse <- nanoTime.map(endTime => endTime - startTime)
    //       elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
    //       _ <- IzLogging.loggerWithContext(ctx).map {
    //         _
    //           .withCustomContext("task" -> name, "elapsed" -> elapsed)
    //           .log(ctx.level)(s"waiting for completion")
    //       }
    //     } yield ()
    //     measurementHandleInterruption(name, startTime) {
    //       for {
    //         fib <- logTask.repeat(Schedule.fixed(interval)).delay(interval).fork
    //         result <- io.onExit(_ => fib.interrupt)
    //       } yield result
    //     }
    //   }
    // }

    // def debug(name: String, logTraceOnError: Boolean = false)(implicit
    //   ctx: LogCtx
    // ): ZIO[R with IzLogging with Clock, E, A] = {
    //   log(name, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
    // }

    // def retryForever[R1 <: R](
    //   retryPolicy: Schedule[R1, Any, Any],
    //   repeatSchedule: Schedule[R1, Any, Any],
    //   retryResetMinDuration: zio.duration.Duration
    // ): ZIO[R1 with Clock, Nothing, Unit] = {
    //   io.either.timed
    //     .flatMap {
    //       case (elapsed, result) =>
    //         result match {
    //           case Right(_) => ZIO.succeed(())
    //           case Left(_) if elapsed > retryResetMinDuration => ZIO.succeed(())
    //           case Left(ex) => ZIO.fail(ex)
    //         }
    //     }
    //     .retry(retryPolicy)
    //     .repeat(repeatSchedule.unit)
    //     .orDieWith(_ => new IllegalStateException("Can't happen with infinite retries"))
    // }

    private def bracketStartMeasurement(name: String, renderResult: A => String, logTraceOnError: Boolean)(implicit
      trace: Trace
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

    private def measurementHandleInterruption[R1, E2, A2](name: String, startTime: Long)(io2: ZIO[R1, E2, A2])(implicit
      trace: Trace
    ) =
      ZIO
        .acquireReleaseExitWith(io2.interruptible.fork) { (fib, exit) =>
          val log =
            for
              elapse <- nanoTime.map(endTime => endTime - startTime)
              elapsed = Nanoseconds(elapse).inBestUnit.rounded(2)
              _ <- ZIO.log("interrupting...") @@ ZIOAspect.annotated("task" -> name, "elapsed" -> elapsed.toString)
            yield ()
          log.when(exit.isInterrupted) *> fib.interrupt
        } { fib =>
          fib.join
        }

  end extension

  // implicit final class ZManagedExtensions[R >: Nothing, E <: Any, A](managed: ZManaged[R, E, A]) {
  //   def logResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
  //     ctx: LogCtx
  //   ): ZManaged[R with IzLogging with Clock, E, A] = {
  //     for {
  //       startTimeRef <- Ref.make[Long](0).toManaged_
  //       value <- managed
  //         .flatMap { value =>
  //           for {
  //             startTime <- ZManaged.fromEffect(startMeasurement(name, result(value)))
  //             _ <- startTimeRef.set(startTime).toManaged_
  //           } yield value
  //         }
  //         .onExit { exit =>
  //           for {
  //             startTime <- startTimeRef.get
  //             _ <- logElapsedTime(
  //               name = name,
  //               startTimeNanos = startTime,
  //               exit = exit,
  //               renderResult = (_: A) => "completed",
  //               logTraceOnError = logTraceOnError
  //             )
  //           } yield ()
  //         }
  //     } yield value
  //   }

  //   def debugResult(name: String, result: A => String, logTraceOnError: Boolean = false)(implicit
  //     ctx: LogCtx
  //   ): ZManaged[R with IzLogging with Clock, E, A] = {
  //     logResult(name, result, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
  //   }

  //   def log(name: String, logTraceOnError: Boolean = false)(implicit
  //     ctx: LogCtx
  //   ): ZManaged[R with IzLogging with Clock, E, A] = {
  //     logResult(name, _ => "started", logTraceOnError)
  //   }

  //   def debug(name: String, logTraceOnError: Boolean = false)(implicit
  //     ctx: LogCtx
  //   ): ZManaged[R with IzLogging with Clock, E, A] = {
  //     log(name, logTraceOnError)(ctx.copy(level = Log.Level.Debug))
  //   }

  // }

  private def startMeasurement(name: String, message: String)(implicit trace: Trace): UIO[Long] =
    for
      time <- nanoTime
      aspect = ZIOAspect.annotated("task", name)
      _ <- aspect(ZIO.logInfo(message))
    yield time

  private def logElapsedTime[E, A](
    name: String,
    startTimeNanos: Long,
    exit: Exit[E, A],
    renderResult: A => String,
    logTraceOnError: Boolean
  )(implicit trace: Trace) = {
    for {
      elapse <- nanoTime.map(endTime => endTime - startTimeNanos)
      elapsed = Nanoseconds(elapse).inBestUnit.rounded(scale = 2)
      _ <- ZIOAspect.annotated("task" -> name, "elapsed" -> elapsed.toString) {
        exit.toEither match
          case Left(FiberFailure(cause)) if cause.isInterrupted =>
            ZIO.log("interrupted")

          case Left(exception @ FiberFailure(cause)) =>
            if (logTraceOnError)
              ZIO.logError(s"failed $exception")
            else
              ZIO.logError("failed") @@ ZIOAspect.annotated("cause", cause.untraced.toString())

          case Left(exception) =>
            if (logTraceOnError)
              ZIO.logError(s"failed $exception")
            else
              ZIO.logError("failed") @@ ZIOAspect.annotated("message", exception.getMessage())

          case Right(r) =>
            ZIO.log(renderResult(r))
      }
    } yield ()
  }
