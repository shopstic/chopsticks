package dev.chopsticks.fp.util

import java.util.concurrent.{CompletableFuture, CompletionException}

import dev.chopsticks.fp.log_env.LogCtx
import zio.{Fiber, RIO, Task, UIO, ZIO}
import dev.chopsticks.fp.zio_ext._

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

  def raceFirst(tasks: Iterable[(String, Task[Unit])])(implicit logCtx: LogCtx): RIO[MeasuredLogging, Unit] = {
    val list = tasks.map {
      case (name, task) =>
        task.log(name)
    }.toList

    list match {
      case head :: tail :: Nil =>
        head.raceFirst(tail)
      case head :: tail =>
        head.run.raceAll(tail.map(_.run)).flatMap(ZIO.done(_)).refailWithTrace
      case Nil =>
        Task.unit
    }
  }
}
