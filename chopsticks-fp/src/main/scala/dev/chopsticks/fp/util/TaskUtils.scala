package dev.chopsticks.fp.util

import java.util.concurrent.{CompletableFuture, CompletionException}
import zio.{Task, ZIO, ZIOAspect}

import scala.concurrent.ExecutionException

object TaskUtils:
  private def catchFromGet(isFatal: Throwable => Boolean): PartialFunction[Throwable, Task[Nothing]] =
    case e: CompletionException =>
      ZIO.fail(e.getCause)
    case e: ExecutionException =>
      ZIO.fail(e.getCause)
    case _: InterruptedException =>
      ZIO.interrupt
    case e if !isFatal(e) =>
      ZIO.fail(e)

  private def unwrapDone[A](isFatal: Throwable => Boolean)(f: CompletableFuture[A]): Task[A] =
    try succeedNowTask(f.get())
    catch catchFromGet(isFatal)

  def fromUninterruptibleCompletableFuture[A](
    name: => String,
    thunk: => CompletableFuture[A]
  ): Task[A] =
    ZIO.attempt(thunk).flatMap { future =>
      ZIO.isFatal.flatMap { isFatal =>
        ZIO.suspendSucceed {
          if (future.isDone) {
            unwrapDone(isFatal)(future)
          }
          else {
            val task = ZIO
              .async { (cb: Task[A] => Unit) =>
                val _ = future.whenComplete { (v, e) =>
                  if (e == null) {
                    cb(ZIO.succeed(v))
                  }
                  else {
                    cb(catchFromGet(isFatal).lift(e).getOrElse(ZIO.die(e)))
                  }
                }
              }

            task
              .onInterrupt {
                for {
                  loggingFib <- ZIO
                    .logWarning(s"$name is uninterruptible and has not completed after 5 seconds!")
                    .delay(java.time.Duration.ofSeconds(5))
                    .fork
                    .interruptible @@ ZIOAspect.annotated("name", name)
                  _ <- task.ignore.onExit(_ => loggingFib.interrupt)
                } yield ()
              }
          }
        }
      }
    }

  def fromCancellableCompletableFuture[A](thunk: => CompletableFuture[A]): Task[A] =
    ZIO.attempt(thunk).flatMap { future =>
      ZIO.isFatal.flatMap { isFatal =>
        ZIO.suspendSucceed {
          ZIO.asyncInterrupt { cb =>
            if (future.isDone) {
              Right(unwrapDone(isFatal)(future))
            }
            else {
              val _ = future.whenComplete { (v, e) =>
                if (e == null) {
                  cb(ZIO.succeed(v))
                }
                else {
                  cb(catchFromGet(isFatal).lift(e).getOrElse(ZIO.die(e)))
                }
              }

              Left(ZIO.succeed(future.cancel(true)) *> ZIO.fromCompletableFuture(future).ignore)
            }
          }
        }
      }
    }

  private def succeedNowTask[A](value: A): Task[A] = ZIO.succeed(value)

end TaskUtils
