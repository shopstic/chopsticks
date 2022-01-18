package dev.chopsticks.stream

import akka.stream.KillSwitch
import akka.stream.SubscriptionWithCancelException.NonFailureCancellation
import akka.stream.scaladsl.RunnableGraph
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import zio.{RIO, Task, UIO, ZIO}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ZAkkaGraph {
  implicit final class InterruptibleGraphOps[Mat <: KillSwitch, Ret](graph: => RunnableGraph[(Mat, Future[Ret])]) {
    def interruptibleRun(graceful: Boolean = true)(implicit
      logCtx: LogCtx
    ): RIO[IzLogging with AkkaEnv, Ret] = {
      for {
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        logger <- ZIO.access[IzLogging](_.get.loggerWithCtx(logCtx))
        ret <- {
          import akkaSvc.{actorSystem, dispatcher}
          val (ks, future) = graph.run()
          val task = future.value
            .fold {
              Task.effectAsync { cb: (Task[Ret] => Unit) =>
                future.onComplete {
                  case Success(a) => cb(Task.succeed(a))
                  case Failure(t) => cb(Task.fail(t))
                }
              }
            }(Task.fromTry(_))

          task.onInterrupt {
            val wait = task.fold(
              {
                case _: NonFailureCancellation =>
                case e => logger.error(s"Graph interrupted ($graceful) which led to: ${e.toString -> "exception"}")
              },
              _ => ()
            )

            UIO {
              if (graceful) ks.shutdown()
              else ks.abort(new InterruptedException("Stream (interruptibleRun) was interrupted"))
            } *> wait
          }
        }
      } yield ret
    }
  }

  implicit final class UninterruptibleGraphWithMatOps[Mat, Ret](graph: => RunnableGraph[(Mat, Future[Ret])]) {
    def uninterruptibleRun: RIO[AkkaEnv, (Mat, Ret)] = {
      for {
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        ret <- {
          import akkaSvc.actorSystem
          Task.fromFuture { implicit ec =>
            val (mat, future) = graph.run()
            future.map(ret => mat -> ret)
          }
        }
      } yield ret
    }
  }

  implicit final class UninterruptibleGraphOps[Ret](graph: => RunnableGraph[Future[Ret]]) {
    def uninterruptibleRun: RIO[AkkaEnv, Ret] = {
      for {
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        ret <- {
          import akkaSvc.actorSystem
          Task.fromFuture(_ => graph.run())
        }
      } yield ret
    }
  }
}
