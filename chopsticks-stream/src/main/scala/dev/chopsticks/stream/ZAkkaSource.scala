package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream._
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext.TaskExtensions
import shapeless.<:!<
import zio._

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ZAkkaSource {
  implicit final class InterruptibleZAkkaSourceOps[-R, V, Mat <: KillSwitch](zSource: => ZAkkaSource[
    R,
    Throwable,
    V,
    Mat
  ]) {
    def interruptibleRunIgnore(graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[IzLogging with AkkaEnv with R, Throwable, Unit] = {
      interruptibleRunWith(Sink.ignore, graceful).unit
    }

    def interruptibleRunWith[Out](sink: => Sink[V, Future[Out]], graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[IzLogging with AkkaEnv with R, Throwable, Out] = {
      for {
        graph <- zSource.make.map(_.toMat(sink)(Keep.both))
        akkaService <- ZIO.access[AkkaEnv](_.get)
        logger <- ZIO.access[IzLogging](_.get.loggerWithCtx(ctx))
        ret <- {
          import akkaService.{actorSystem, dispatcher}
          val (ks, f) = graph.run()
          val task = f.value
            .fold {
              Task.effectAsync { cb: (Task[Out] => Unit) =>
                f.onComplete {
                  case Success(a) => cb(Task.succeed(a))
                  case Failure(t) => cb(Task.fail(t))
                }
              }
            }(Task.fromTry(_))

          task.onInterrupt(
            UIO {
              if (graceful) ks.shutdown()
              else ks.abort(new InterruptedException("Stream (interruptibleGraph) was interrupted"))
            } *> task.fold(
              e => logger.error(s"Graph interrupted ($graceful) which led to: ${e.getMessage -> "exception"}"),
              _ => ()
            )
          )
        }
      } yield ret
    }
  }

  implicit final class UninterruptibleZAkkaSourceOps[-R, V, Mat](zSource: => ZAkkaSource[R, Throwable, V, Mat])(implicit
    notKillSwitch: Mat <:!< KillSwitch
  ) {
    def runIgnore: ZIO[AkkaEnv with R, Throwable, Unit] = {
      runWith(Sink.ignore).unit
    }

    def runWith[Out](sink: => Graph[SinkShape[V], Future[Out]]): ZIO[AkkaEnv with R, Throwable, Out] = {
      for {
        graph <- zSource.make.map(_.toMat(sink)(Keep.right))
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        ret <- Task.fromFuture { _ =>
          import akkaSvc.actorSystem
          graph.run()
        }
      } yield ret
    }

    def interruptibleRunIgnore(graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[IzLogging with AkkaEnv with R, Throwable, Unit] = {
      new InterruptibleZAkkaSourceOps(zSource.interruptible).interruptibleRunIgnore(graceful)
    }

    def interruptibleRunWith[Out](sink: => Sink[V, Future[Out]], graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): ZIO[IzLogging with AkkaEnv with R, Throwable, Out] = {
      new InterruptibleZAkkaSourceOps(zSource.interruptible).interruptibleRunWith(sink, graceful)
    }
  }

  implicit final class SourceToZAkkaSource[V, Mat](source: => Source[V, Mat]) {
    def toZAkkaSource: ZAkkaSource[Any, Nothing, V, Mat] = ZAkkaSource(source)
  }

  def apply[R, E, V, Mat](make: ZIO[R, E, Source[V, Mat]]): ZAkkaSource[R, E, V, Mat] = new ZAkkaSource(make)
  def apply[R, V, Mat](source: => Source[V, Mat]): ZAkkaSource[R, Nothing, V, Mat] =
    new ZAkkaSource(ZIO.succeed(source))

  def from[R, E, V, Mat](make: ZIO[R, E, Source[V, Mat]]): ZAkkaSource[R, E, V, Mat] = new ZAkkaSource(make)
  def from[R, V, Mat](source: => Source[V, Mat]): ZAkkaSource[R, Nothing, V, Mat] = new ZAkkaSource(ZIO.succeed(source))
}

final class ZAkkaSource[-R, +E, V, Mat] private (val make: ZIO[R, E, Source[V, Mat]]) {
  def mapAsync[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out]): ZAkkaSource[R1, E, Out, Mat] = {
    new ZAkkaSource(
      make
        .flatMap { source =>
          ZIO.runtime[R1].map { implicit rt =>
            source
              .mapAsync(parallelism) { a =>
                runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
              }
          }
        }
    )
  }

  def mapAsyncUnordered[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out]): ZAkkaSource[R1, E, Out, Mat] = {
    new ZAkkaSource(
      make
        .flatMap { source =>
          ZIO.runtime[R1].map { implicit rt =>
            source
              .mapAsyncUnordered(parallelism) { a =>
                runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
              }
          }
        }
    )
  }

  def interruptibleMapAsync[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out])
    : ZAkkaSource[R1 with AkkaEnv, E, Out, Mat] = {
    new ZAkkaSource[R1 with AkkaEnv, E, Out, Mat](
      make
        .flatMap { source =>
          ZIO.runtime[R1 with AkkaEnv].map { implicit rt =>
            val env = rt.environment
            val akkaService = env.get[AkkaEnv.Service]
            import akkaService._

            val flow = Flow
              .lazyFutureFlow(() => {
                val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])

                Future
                  .successful(
                    Flow[V]
                      .mapAsync(parallelism) { a =>
                        val interruptibleTask = for {
                          fib <- runTask(a).fork
                          c <- (completionPromise.await *> fib.interrupt).fork
                          ret <- fib.join
                          _ <- c.interrupt
                        } yield ret

                        interruptibleTask.unsafeRunToFuture
                      }
                      .watchTermination() { (_, f) =>
                        f.onComplete { _ =>
                          val _ = rt.unsafeRun(completionPromise.succeed(()))
                        }
                        akka.NotUsed
                      }
                  )
              })

            source.via(flow)
          }
        }
    )
  }

  def interruptibleMapAsyncUnordered[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out])
    : ZAkkaSource[R1 with AkkaEnv, E, Out, Mat] = {
    new ZAkkaSource[R1 with AkkaEnv, E, Out, Mat](
      make
        .flatMap { source =>
          ZIO.runtime[R1 with AkkaEnv].map { implicit rt =>
            val env = rt.environment
            val akkaService = env.get[AkkaEnv.Service]
            import akkaService._

            val flow = Flow
              .lazyFutureFlow(() => {
                val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])

                Future
                  .successful(
                    Flow[V]
                      .mapAsyncUnordered(parallelism) { a =>
                        val interruptibleTask = for {
                          fib <- runTask(a).fork
                          c <- (completionPromise.await *> fib.interrupt).fork
                          ret <- fib.join
                          _ <- c.interrupt
                        } yield ret

                        interruptibleTask.unsafeRunToFuture
                      }
                      .watchTermination() { (_, f) =>
                        f.onComplete { _ =>
                          val _ = rt.unsafeRun(completionPromise.succeed(()))
                        }
                        akka.NotUsed
                      }
                  )
              })

            source.via(flow)
          }
        }
    )
  }

  def via[Out](flow: => Graph[FlowShape[V, Out], Any]): ZAkkaSource[R, E, Out, Mat] = {
    viaMat(flow)(Keep.left)
  }

  def viaChain[Out](makeFlow: Flow[V, V, NotUsed] => Graph[FlowShape[V, Out], Any]): ZAkkaSource[R, E, Out, Mat] = {
    viaChainMat(makeFlow)(Keep.left)
  }

  def viaMat[Out, Mat2, Mat3](flow: => Graph[FlowShape[V, Out], Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R, E, Out, Mat3] = {
    viaChainMat(_ => flow)(combine)
  }

  def viaChainMat[Out, Mat2, Mat3](makeFlow: Flow[V, V, NotUsed] => Graph[FlowShape[V, Out], Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R, E, Out, Mat3] = {
    new ZAkkaSource(
      make
        .map { source =>
          source.viaMat(makeFlow(Flow[V]))(combine)
        }
    )
  }

  def viaM[R1 <: R, E1 >: E, Out](makeFlow: ZIO[R1, E1, Graph[FlowShape[V, Out], Any]])
    : ZAkkaSource[R1, E1, Out, Mat] = {
    viaMatM(makeFlow)(Keep.left)
  }

  def viaChainM[R1 <: R, E1 >: E, Out](makeFlow: Flow[V, V, NotUsed] => ZIO[R1, E1, Graph[FlowShape[V, Out], Any]])
    : ZAkkaSource[R1, E1, Out, Mat] = {
    viaMatM(makeFlow(Flow[V]))(Keep.left)
  }

  def viaMatM[R1 <: R, E1 >: E, Out, Mat2, Mat3](makeFlow: ZIO[R1, E1, Graph[FlowShape[V, Out], Mat2]])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Out, Mat3] = {
    new ZAkkaSource(
      for {
        source <- make
        flow <- makeFlow
      } yield {
        source.viaMat(flow)(combine)
      }
    )
  }

  def viaChainMatM[R1 <: R, E1 >: E, Out, Mat2, Mat3](makeFlow: Flow[V, V, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[V, Out], Mat2]
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Out, Mat3] = {
    viaMatM(makeFlow(Flow[V]))(combine)
  }

  def interruptible: ZAkkaSource[R, E, V, UniqueKillSwitch] = {
    new ZAkkaSource(
      make.map(_.viaMat(KillSwitches.single)(Keep.right))
    )
  }
}
