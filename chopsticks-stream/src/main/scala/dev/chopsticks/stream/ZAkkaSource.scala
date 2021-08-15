package dev.chopsticks.stream

import akka.NotUsed
import akka.actor.Status
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream._
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext.TaskExtensions
import dev.chopsticks.stream.ZAkkaGraph._
import zio.{Cause, Exit, IO, NeedsEnv, RIO, Task, UIO, URIO, ZIO, ZScope}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import shapeless.<:!<

object ZAkkaSource {
  implicit final class InterruptibleZAkkaSourceOps[-R, +V, +Mat <: KillSwitch](zSource: => ZAkkaSource[
    R,
    Throwable,
    V,
    Mat
  ]) {
    def interruptibleRunIgnore(graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): RIO[IzLogging with AkkaEnv with R, Unit] = {
      interruptibleRunWith(Sink.ignore, graceful).unit
    }

    def interruptibleRunWith[Out](sink: => Graph[SinkShape[V], Future[Out]], graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): RIO[IzLogging with AkkaEnv with R, Out] = {
      zSource.to(sink).flatMap(_.interruptibleRun(graceful))
    }
  }

  implicit final class UninterruptibleZAkkaSourceOps[-R, +V, +Mat](zSource: => ZAkkaSource[R, Throwable, V, Mat])(
    implicit notKillSwitch: Mat <:!< KillSwitch
  ) {
    def uninterruptibleRunIgnore: ZIO[AkkaEnv with R, Throwable, Unit] = {
      uninterruptibleRunWith(Sink.ignore).unit
    }

    def uninterruptibleRunWith[Out](sink: => Graph[SinkShape[V], Future[Out]]): RIO[AkkaEnv with R, (Mat, Out)] = {
      zSource.to(sink).flatMap(_.uninterruptibleRun)
    }

    def interruptibleRunIgnore(graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): RIO[IzLogging with AkkaEnv with R, Unit] = {
      new InterruptibleZAkkaSourceOps(zSource.interruptible).interruptibleRunIgnore(graceful)
    }

    def interruptibleRunWith[Out](
      sink: => Sink[V, Future[Out]],
      graceful: Boolean = true
    )(implicit
      ctx: LogCtx
    ): RIO[IzLogging with AkkaEnv with R, Out] = {
      new InterruptibleZAkkaSourceOps(zSource.interruptible).interruptibleRunWith(sink, graceful)
    }
  }

  implicit final class SourceToZAkkaSource[+V, +Mat](source: => Source[V, Mat]) {
    def toZAkkaSource: UAkkaSource[V, Mat] = {
      new ZAkkaSource(_ => ZIO.succeed(source))
    }
  }

  def interruptibleLazySource[R, V](
    effect: RIO[R, V]
  ): URIO[AkkaEnv with R, Source[V, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaSvc = env.get[AkkaEnv.Service]
      import akkaSvc._

      Source
        .lazySource(() => {
          val completionPromise = scala.concurrent.Promise[Either[Throwable, V]]()
          val task = effect.either race Task.fromFuture(_ => completionPromise.future)

          Source
            .future(task.flatMap(Task.fromEither(_)).unsafeRunToFuture)
            .watchTermination() { (_, f) =>
              f.onComplete { _ =>
                val _ =
                  completionPromise.success(Left(new InterruptedException("interruptibleLazySource was interrupted")))
              }
              NotUsed
            }
        })
    }
  }

  def recursiveSource[R, Out, State](seed: => RIO[R, State], nextState: (State, Out) => State)(
    makeSource: State => Source[Out, NotUsed]
  ): URIO[AkkaEnv with R, Source[Out, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaSvc = env.get[AkkaEnv.Service]
      import akkaSvc.{actorSystem, dispatcher}

      Source
        .lazyFuture(() => {
          seed.map { seedState =>
            val completionMatcher: PartialFunction[Any, CompletionStrategy] = {
              case Status.Success(s: CompletionStrategy) => s
              case Status.Success(_) => CompletionStrategy.draining
            }

            val failureMatcher: PartialFunction[Any, Throwable] = {
              case Status.Failure(cause) => cause
            }

            val (actorRef, source) = Source
              .actorRef[State](
                completionMatcher,
                failureMatcher,
                1,
                OverflowStrategy.fail
              )
              .preMaterialize()

            actorRef ! seedState

            source
              .flatMapConcat { state =>
                val (future, subSource) = makeSource(state)
                  .viaMat(LastStateFlow[Out, State, State](state, nextState, identity))(Keep.right)
                  .preMaterialize()

                subSource ++ Source.futureSource(future.map {
                  case (state, Success(_)) =>
                    actorRef ! state
                    Source.empty[Out]
                  case (_, Failure(ex)) =>
                    actorRef ! Status.Failure(ex)
                    Source.failed[Out](ex)
                })
              }
          }.unsafeRunToFuture
        })
        .flatMapConcat(identity)
    }
  }
}

final class ZAkkaSource[-R, +E, +Out, +Mat](val make: ZScope[Exit[Any, Any]] => ZIO[
  R,
  E,
  Source[Out, Mat]
]) {
  def provide(r: R)(implicit ev: NeedsEnv[R]): IO[E, ZAkkaSource[Any, E, Out, Mat]] = {
    requireEnv.provide(r)
  }

  def requireEnv: ZIO[R, E, ZAkkaSource[Any, E, Out, Mat]] = {
    ZRunnable(make).toZIO.map(new ZAkkaSource(_))
  }

  def toMat[Ret, Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => (Mat3, Future[Ret]))
    : ZIO[R with AkkaEnv, E, RunnableGraph[(Mat3, Future[Ret])]] = {
    for {
      scope <- ZScope.make[Exit[Any, Any]]
      runtime <- ZIO.runtime[AkkaEnv]
      source <- make(scope.scope)
    } yield {
      source
        .toMat(sink) { (mat, mat2) =>
          implicit val rt: zio.Runtime[AkkaEnv] = runtime
          implicit val ec: ExecutionContextExecutor = runtime.environment.get[AkkaEnv.Service].dispatcher

          val (mat3, future) = combine(mat, mat2)
          mat3 -> future
            .transformWith { result =>
              val finalizer = result match {
                case Failure(exception) =>
                  scope.close(Exit.Failure(Cause.fail(exception)))
                case Success(value) =>
                  scope.close(Exit.Success(value))
              }
              rt.unsafeRunToFuture(
                UIO(println("BEFORE finalizer")) *> finalizer.tap(r => UIO(println(s"AFTER finalizer $r")))
              ).transform(_ => result)
            }
        }
    }
  }

  def to[Ret](sink: Graph[SinkShape[Out], Future[Ret]]): ZIO[R with AkkaEnv, E, RunnableGraph[(Mat, Future[Ret])]] = {
    toMat(sink)(Keep.both)
  }

  def mapAsync[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaSource[R1 with AkkaEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsync(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def mapAsyncUnordered[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaSource[R1 with AkkaEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsyncUnordered(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def switchFlatMapConcat[R1 <: R, Next](runTask: Out => RIO[R1, Graph[SourceShape[Next], Any]])
    : ZAkkaSource[R1 with AkkaEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].switchFlatMapConcat(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def viaZAkkaFlow[R1 <: R, E1 >: E, Next, Mat2, Mat3](next: ZAkkaFlow[R1, E1, Out @uncheckedVariance, Next, Mat2])
    : ZAkkaSource[R1, E1, Next, Mat] = {
    viaZAkkaFlowMat(next)(Keep.left)
  }

  def viaZAkkaFlowMat[R1 <: R, E1 >: E, Next, Mat2, Mat3](next: ZAkkaFlow[R1, E1, Out @uncheckedVariance, Next, Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Next, Mat3] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        nextFlow <- next.make(scope)
      } yield {
        source
          .viaMat(nextFlow)(combine)
      }
    })
  }

  def viaBuilderMat[Next, Mat2, Mat3](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => Graph[
    FlowShape[Out, Next],
    Mat2
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R, E, Next, Mat3] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
      } yield {
        source
          .viaMat(makeFlow(Flow[Out]))(combine)
      }
    })
  }

  def viaMat[Next, Mat2, Mat3](flow: => Graph[FlowShape[Out, Next], Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R, E, Next, Mat3] = {
    viaBuilderMat(_ => flow)(combine)
  }

  def via[Next](flow: => Graph[FlowShape[Out, Next], Any]): ZAkkaSource[R, E, Next, Mat] = {
    viaMat(flow)(Keep.left)
  }

  def viaBuilder[Next](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => Graph[FlowShape[Out, Next], Any])
    : ZAkkaSource[R, E, Next, Mat] = {
    viaBuilderMat(makeFlow)(Keep.left)
  }

  def viaM[R1 <: R, E1 >: E, Next](makeFlow: ZIO[R1, E1, Graph[FlowShape[Out, Next], Any]])
    : ZAkkaSource[R1, E1, Next, Mat] = {
    viaMatM(makeFlow)(Keep.left)
  }

  def viaBuilderM[R1 <: R, E1 >: E, Next](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Any]
  ]): ZAkkaSource[R1, E1, Next, Mat] = {
    viaMatM(makeFlow(Flow[Out]))(Keep.left)
  }

  def viaMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: ZIO[R1, E1, Graph[FlowShape[Out, Next], Mat2]])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Next, Mat3] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        nextFlow <- makeFlow
      } yield {
        source
          .viaMat(nextFlow)(combine)
      }
    })
  }

  def viaBuilderMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Mat2]
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Next, Mat3] = {
    viaMatM(makeFlow(Flow[Out]))(combine)
  }

  def interruptible: ZAkkaSource[R, E, Out, UniqueKillSwitch] = {
    new ZAkkaSource(scope => {
      for {
        flow <- make(scope)
      } yield {
        flow
          .viaMat(KillSwitches.single)(Keep.right)
      }
    })
  }
}
