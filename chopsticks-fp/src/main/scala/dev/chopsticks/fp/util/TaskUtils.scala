package dev.chopsticks.fp.util

import java.util.concurrent.{CompletableFuture, CompletionException}
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import zio.{RIO, Task, UIO, ZIO}

import scala.concurrent.ExecutionException

object TaskUtils {
  private def catchFromGet(isFatal: Throwable => Boolean): PartialFunction[Throwable, Task[Nothing]] = {
    case e: CompletionException =>
      Task.fail(e.getCause)
    case e: ExecutionException =>
      Task.fail(e.getCause)
    case _: InterruptedException =>
      Task.interrupt
    case e if !isFatal(e) =>
      Task.fail(e)
  }

  private def unwrapDone[A](isFatal: Throwable => Boolean)(f: CompletableFuture[A]): Task[A] = {
    try {
      succeedNowTask(f.get())
    }
    catch catchFromGet(isFatal)
  }

  def fromUninterruptibleCompletableFuture[A](
    name: => String,
    thunk: => CompletableFuture[A]
  ): RIO[MeasuredLogging, A] = {
    ZIO.effect(thunk).flatMap { future =>
      ZIO.effectSuspendTotalWith { (p, _) =>
        if (future.isDone) {
          unwrapDone(p.fatal)(future)
        }
        else {
          val task = Task
            .effectAsync { (cb: Task[A] => Unit) =>
              val _ = future.whenComplete { (v, e) =>
                if (e == null) {
                  cb(Task.succeed(v))
                }
                else {
                  cb(catchFromGet(p.fatal).lift(e).getOrElse(Task.die(e)))
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
                _ <- task.ignore
                _ <- loggingFib.interrupt
              } yield ()
            }
        }
      }
    }
  }

  def fromCancellableCompletableFuture[A](thunk: => CompletableFuture[A]): Task[A] =
    Task.effect(thunk).flatMap { future =>
      Task.effectSuspendTotalWith { (p, _) =>
        Task.effectAsyncInterrupt[A] { cb =>
          if (future.isDone) {
            Right(unwrapDone(p.fatal)(future))
          }
          else {
            val _ = future.whenComplete { (v, e) =>
              if (e == null) {
                cb(Task.succeed(v))
              }
              else {
                cb(catchFromGet(p.fatal).lift(e).getOrElse(Task.die(e)))
              }
            }

            Left(UIO(future.cancel(true)) *> ZIO.fromCompletableFuture(future).ignore)
          }
        }
      }
    }

  private def succeedNowTask[A](value: A): Task[A] = Task.succeed(value)
}
