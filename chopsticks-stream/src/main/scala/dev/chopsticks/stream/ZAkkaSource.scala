package dev.chopsticks.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Status
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext.TaskExtensions
import dev.chopsticks.stream.ZAkkaGraph._
import eu.timepit.refined.types.numeric.PosInt
import org.reactivestreams.Publisher
import shapeless.<:!<
import zio.stream.ZStream
import zio.{RIO, URIO, Unsafe, ZIO}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import zio.interop.reactivestreams.Adapters.streamToPublisher
import eu.timepit.refined.auto._

object ZAkkaSource {
  implicit final class InterruptibleZAkkaSourceOps[-R, +V, +Mat <: KillSwitch](zSource: => ZAkkaSource[
    R,
    Throwable,
    V,
    Mat
  ]) {
    def interruptibleRunIgnore(graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): RIO[IzLogging with PekkoEnv with R, Unit] = {
      interruptibleRunWith(Sink.ignore, graceful).unit
    }

    def interruptibleRunWith[Out](sink: => Graph[SinkShape[V], Future[Out]], graceful: Boolean = true)(implicit
      ctx: LogCtx
    ): RIO[IzLogging with PekkoEnv with R, Out] = {
      zSource.to(sink).flatMap(_.interruptibleRun(graceful))
    }
  }

  implicit final class UninterruptibleZAkkaSourceOps[-R, +V, +Mat](zSource: => ZAkkaSource[R, Throwable, V, Mat])(
    implicit notKillSwitch: Mat <:!< KillSwitch
  ) {
    def uninterruptibleRunIgnore: ZIO[PekkoEnv with R, Throwable, Unit] = {
      uninterruptibleRunWith(Sink.ignore).unit
    }

    def uninterruptibleRunWith[Out](sink: => Graph[SinkShape[V], Future[Out]]): RIO[PekkoEnv with R, (Mat, Out)] = {
      zSource.to(sink).flatMap(_.uninterruptibleRun)
    }
  }

  implicit final class SourceToZAkkaSource[+V, +Mat](source: => Source[V, Mat]) {
    def toZAkkaSource: UAkkaSource[V, Mat] = {
      new ZAkkaSource(_ => ZIO.succeed(source))
    }
  }

  implicit class ZStreamToZAkkaSource[R, E <: Throwable, O](stream: ZStream[R, E, O]) {
    // todo [migration] check if this impl is enough or should we bring back the previous one (we probably need to include the scope here)
    def toZAkkaSource(bufferSize: PosInt = 1): ZAkkaSource[R, Nothing, O, Future[NotUsed]] = new ZAkkaSource(
      make = (_: ZAkkaScope) => {
        streamToPublisher(stream).map { publisher: Publisher[O] =>
          Source.lazySource { () =>
            Source.fromPublisher(publisher).addAttributes(Attributes.inputBuffer(bufferSize.value, bufferSize.value))
          }
        }
      }
    )
  }

  def interruptibleLazySource[R, V](
    effect: RIO[R, V]
  ): URIO[PekkoEnv with R, Source[V, Future[NotUsed]]] = {
    ZIO.runtime[PekkoEnv with R].map { implicit rt =>
      val env = rt.environment
      val pekkoSvc = env.get[PekkoEnv]
      import pekkoSvc._

      Source
        .lazySource(() => {
          val completionPromise = scala.concurrent.Promise[Either[Throwable, V]]()
          val task = effect.either race ZIO.fromFuture(_ => completionPromise.future)

          Source
            .future(task.flatMap(ZIO.fromEither(_)).unsafeRunToFuture)
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
  ): URIO[PekkoEnv with R, Source[Out, NotUsed]] = {
    ZIO.runtime[PekkoEnv with R].map { implicit rt =>
      val env = rt.environment
      val pekkoSvc = env.get[PekkoEnv]
      import pekkoSvc.{actorSystem, dispatcher}

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

  def unfoldAsyncWithScope[S, R, O](seed: S)(runTask: (S, ZAkkaScope) => RIO[R, Option[(S, O)]])
    : ZAkkaSource[R with PekkoEnv, Nothing, O, NotUsed] = {
    new ZAkkaSource(scope => {
      for {
        runtime <- ZIO.runtime[R with PekkoEnv]
        promise <- zio.Promise.make[Nothing, Unit]
      } yield {
        implicit val rt: zio.Runtime[R with PekkoEnv] = runtime
        implicit val ec: ExecutionContextExecutor = rt.environment.get[PekkoEnv].dispatcher

        Source
          .unfoldAsync(seed) { state =>
            val task = for {
              fib <- scope.fork(runTask(state, scope))
              interruptFib <- scope.fork(promise.await *> fib.interrupt)
              ret <- fib.join.ensuring(interruptFib.interrupt)
            } yield ret

            task.unsafeRunToFuture
          }
          .watchTermination() { (mat, future) =>
            future.onComplete(_ =>
              Unsafe.unsafe { implicit unsafe =>
                rt.unsafe.run(promise.succeed(())).getOrThrowFiberFailure()
              }
            )
            mat
          }
      }
    })
  }

  def unfoldAsync[S, R, O](seed: S)(runTask: S => RIO[R, Option[(S, O)]])
    : ZAkkaSource[R with PekkoEnv, Nothing, O, NotUsed] = {
    unfoldAsyncWithScope(seed) { (state, _) =>
      runTask(state)
    }
  }
}

final class ZAkkaSource[-R, +E, +Out, +Mat](val make: ZAkkaScope => ZIO[
  R,
  E,
  Source[Out, Mat]
]) {
  // todo [migration]
//  def provide(r: R)(implicit ev: NeedsEnv[R]): IO[E, ZAkkaSource[Any, E, Out, Mat]] = {
//    toZIO.provide(r)
//  }

  def toZIO: ZIO[R, E, ZAkkaSource[Any, E, Out, Mat]] = {
    ZIO.environmentWith[R] { env =>
      val newMake = (scope: ZAkkaScope) => make(scope).provideEnvironment(env)
      new ZAkkaSource(newMake)
    }
  }

  def toMat[Ret, Mat2, Mat3](sink: Graph[SinkShape[Out], Mat2])(combine: (Mat, Mat2) => (Mat3, Future[Ret]))
    : ZIO[R with PekkoEnv, E, RunnableGraph[(Mat3, Future[Ret])]] = {
    for {
      scope <- ZAkkaScope.make
      runtime <- ZIO.runtime[PekkoEnv]
      source <- make(scope)
    } yield {
      source
        .toMat(sink) { (mat, mat2) =>
          implicit val rt: zio.Runtime[PekkoEnv] = runtime
          implicit val ec: ExecutionContextExecutor = runtime.environment.get[PekkoEnv].dispatcher

          val (mat3, future) = combine(mat, mat2)
          mat3 -> future
            .transformWith { result =>
              Unsafe.unsafe { implicit unsafe =>
                rt.unsafe.runToFuture(scope.close()).transform(_.flatMap(_ => result))
              }
            }
        }
    }
  }

  def to[Ret](sink: Graph[SinkShape[Out], Future[Ret]]): ZIO[R with PekkoEnv, E, RunnableGraph[(Mat, Future[Ret])]] = {
    toMat(sink)(Keep.both)
  }

  def scanAsync[R1 <: R, Next](zero: Next)(runTask: (Next, Out) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].scanAsync(zero)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def scanAsyncWithScope[R1 <: R, Next](zero: Next)(runTask: (Next, Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].scanAsyncWithScope(zero)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def foldAsync[R1 <: R, Next](zero: Next)(runTask: (Next, Out) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].foldAsync(zero)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def foldAsyncWithScope[R1 <: R, Next](zero: Next)(runTask: (Next, Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].foldAsyncWithScope(zero)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def mapAsync[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsync(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def mapAsyncWithScope[R1 <: R, Next](parallelism: Int)(runTask: (Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsyncWithScope(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def mapAsyncUnordered[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsyncUnordered(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def mapAsyncUnorderedWithScope[R1 <: R, Next](parallelism: Int)(runTask: (Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        flow <- ZAkkaFlow[Out].mapAsyncUnorderedWithScope(parallelism)(runTask).make(scope)
      } yield {
        source.via(flow)
      }
    })
  }

  def switchFlatMapConcat[R1 <: R, Next](runTask: Out => RIO[R1, Graph[SourceShape[Next], Any]])
    : ZAkkaSource[R1 with PekkoEnv, E, Next, Mat] = {
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

  def viaBuilderWithScopeMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: (
    Flow[Out @uncheckedVariance, Out, NotUsed],
    ZAkkaScope
  ) => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Mat2]
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R1, E1, Next, Mat3] = {
    new ZAkkaSource(scope => {
      for {
        source <- make(scope)
        nextFlow <- makeFlow(Flow[Out], scope)
      } yield {
        source
          .viaMat(nextFlow)(combine)
      }
    })
  }

  def killSwitch: ZAkkaSource[R, E, Out, UniqueKillSwitch] = {
    new ZAkkaSource(scope => {
      for {
        flow <- make(scope)
      } yield {
        flow
          .viaMat(KillSwitches.single)(Keep.right)
      }
    })
  }

  @deprecated("Use .killSwitch instead", since = "3.4.0")
  def interruptible: ZAkkaSource[R, E, Out, UniqueKillSwitch] = killSwitch
}
