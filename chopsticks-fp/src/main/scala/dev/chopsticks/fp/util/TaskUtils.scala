package dev.chopsticks.fp.util

import java.util.concurrent.{CompletableFuture, CompletionException}

import dev.chopsticks.fp.iz_logging.LogCtx
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

            Left(UIO {
              future.cancel(true)
            })
          }
        }
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

  private def succeedNowTask[A](value: A): Task[A] = Task.succeed(value)
}
