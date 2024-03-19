package dev.chopsticks.fp.util

import java.util.concurrent.{CompletableFuture, CompletionException}
import dev.chopsticks.fp.iz_logging.IzLogging
import zio.{RIO, Task, UIO, ZIO}

import scala.concurrent.ExecutionException

object TaskUtils {
  private def catchFromGet(isFatal: UIO[Throwable => Boolean]): Throwable => Task[Nothing] = {
    case e: CompletionException =>
      ZIO.fail(e.getCause)
    case e: ExecutionException =>
      ZIO.fail(e.getCause)
    case _: InterruptedException =>
      ZIO.interrupt
    case e =>
      isFatal.flatMap { pred =>
        if (!pred(e)) ZIO.fail(e)
        else ZIO.die(e)
      }
  }

  private def unwrapDone[A](isFatal: UIO[Throwable => Boolean])(f: CompletableFuture[A]): Task[A] = {
    try {
      succeedNowTask(f.get())
    }
    catch { case e: Throwable => catchFromGet(isFatal)(e) }
  }

  def fromUninterruptibleCompletableFuture[A](
    name: => String,
    thunk: => CompletableFuture[A]
  ): RIO[IzLogging, A] = {
    ZIO.attempt(thunk).flatMap { future =>
      ZIO.suspendSucceed {
        if (future.isDone) {
          unwrapDone(ZIO.isFatal)(future)
        }
        else {
          val task = ZIO
            .async { (cb: Task[A] => Unit) =>
              val _ = future.whenComplete { (v, e) =>
                if (e == null) {
                  cb(ZIO.succeed(v))
                }
                else {
                  cb(catchFromGet(ZIO.isFatal)(e))
                }
              }
            }

          task
            .onInterrupt {
              for {
                loggingFib <- IzLogging
                  .zioLogger
                  .flatMap(_.warn(s"$name is uninterruptible and has not completed after 5 seconds!"))
                  .delay(java.time.Duration.ofSeconds(5))
                  .fork
                  .interruptible
                _ <- task.ignore.onExit(_ => loggingFib.interrupt)
              } yield ()
            }
        }
      }
    }
  }

  def fromCancellableCompletableFuture[A](thunk: => CompletableFuture[A]): Task[A] =
    ZIO.attempt(thunk).flatMap { future =>
      ZIO.suspendSucceed {
        ZIO.asyncInterrupt[Any, Throwable, A] { cb =>
          if (future.isDone) {
            Right(unwrapDone(ZIO.isFatal)(future))
          }
          else {
            val _ = future.whenComplete { (v, e) =>
              if (e == null) {
                cb(ZIO.succeed(v))
              }
              else {
                cb(catchFromGet(ZIO.isFatal)(e))
              }
            }

            Left(ZIO.succeed(future.cancel(true)) *> ZIO.fromCompletableFuture(future).ignore)
          }
        }
      }
    }

  private def succeedNowTask[A](value: A): Task[A] = ZIO.succeed(value)
}
