package dev.chopsticks.fp.zio_ext

import java.util.concurrent.{CompletableFuture, CompletionException}

import zio.{Task, UIO}

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
      Task.succeed(f.get())
    }
    catch catchFromGet(isFatal)
  }

  def fromCancellableCompletableFuture[A](thunk: => CompletableFuture[A]): Task[A] = {
    lazy val future = thunk
    Task.effectSuspendTotalWith { (p, _) =>
      Task.effectAsyncInterrupt[A](cb => {
        if (future.isDone) {
          Right(unwrapDone(p.fatal)(future))
        }
        else {
          val _ = future.whenComplete((v, e) => {
            if (e == null) {
              cb(Task.succeed(v))
            }
            else {
              cb(catchFromGet(p.fatal).lift(e).getOrElse(Task.die(e)))
            }
          })

          Left(UIO {
            future.cancel(true)
          })
        }
      })
    }
  }
}
