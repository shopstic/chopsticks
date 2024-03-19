package dev.chopsticks.stream

import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.SubscriptionWithCancelException.NonFailureCancellation
import org.apache.pekko.stream.scaladsl.RunnableGraph
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import zio.{RIO, Task, ZIO}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ZAkkaGraph {
  implicit final class InterruptibleGraphOps[Mat <: KillSwitch, Ret](graph: => RunnableGraph[(Mat, Future[Ret])]) {
    def interruptibleRun(graceful: Boolean = true)(implicit
      logCtx: LogCtx
    ): RIO[IzLogging with PekkoEnv, Ret] = {
      for {
        pekkoSvc <- ZIO.service[PekkoEnv]
        logger <- ZIO.serviceWith[IzLogging](_.loggerWithCtx(logCtx))
        ret <- {
          import pekkoSvc.{actorSystem, dispatcher}
          val (ks, future) = graph.run()
          val task = future.value
            .fold {
              ZIO.async { cb: (Task[Ret] => Unit) =>
                future.onComplete {
                  case Success(a) => cb(ZIO.succeed(a))
                  case Failure(t) => cb(ZIO.fail(t))
                }
              }
            }(ZIO.fromTry(_))

          task.onInterrupt {
            val wait = task.fold(
              {
                case _: NonFailureCancellation =>
                case e =>
                  logger.error(s"Graph interrupted ($graceful) which led to: ${e.toString -> "exception"}")
              },
              _ => ()
            )

            ZIO.succeed {
              if (graceful) ks.shutdown()
              else ks.abort(new InterruptedException("Stream (interruptibleRun) was interrupted"))
            } *> wait
          }
        }
      } yield ret
    }
  }

  implicit final class UninterruptibleGraphWithMatOps[Mat, Ret](graph: => RunnableGraph[(Mat, Future[Ret])]) {
    def uninterruptibleRun: RIO[PekkoEnv, (Mat, Ret)] = {
      for {
        pekkoSvc <- ZIO.service[PekkoEnv]
        ret <- {
          import pekkoSvc.actorSystem
          ZIO.fromFuture { implicit ec =>
            val (mat, future) = graph.run()
            future.map(ret => mat -> ret)
          }
        }
      } yield ret
    }
  }

  implicit final class UninterruptibleGraphOps[Ret](graph: => RunnableGraph[Future[Ret]]) {
    def uninterruptibleRun: RIO[PekkoEnv, Ret] = {
      for {
        pekkoSvc <- ZIO.service[PekkoEnv]
        ret <- {
          import pekkoSvc.actorSystem
          ZIO.fromFuture(_ => graph.run())
        }
      } yield ret
    }
  }
}
