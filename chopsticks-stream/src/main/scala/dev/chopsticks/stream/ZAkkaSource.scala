package dev.chopsticks.stream

import akka.NotUsed
import akka.actor.Status
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext.TaskExtensions
import dev.chopsticks.stream.ZAkkaGraph.{InterruptibleGraphOps, UninterruptibleGraphOps}
import shapeless.<:!<
import zio._

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ZAkkaSource {
  implicit final class InterruptibleZAkkaSourceOps[-R, +V, +Mat <: KillSwitch](zSource: => ZAkkaSource[
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
        ret <- graph.interruptibleRun(graceful)
      } yield ret
    }
  }

  implicit final class UninterruptibleZAkkaSourceOps[-R, +V, +Mat](zSource: => ZAkkaSource[R, Throwable, V, Mat])(
    implicit notKillSwitch: Mat <:!< KillSwitch
  ) {
    def runIgnore: ZIO[AkkaEnv with R, Throwable, Unit] = {
      runWith(Sink.ignore).unit
    }

    def runWith[Out](sink: => Graph[SinkShape[V], Future[Out]]): ZIO[AkkaEnv with R, Throwable, Out] = {
      for {
        graph <- zSource.make.map(_.toMat(sink)(Keep.right))
        ret <- graph.runToIO
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

  implicit final class SourceToZAkkaSource[+V, +Mat](source: => Source[V, Mat]) {
    def toZAkkaSource: ZAkkaSource[Any, Nothing, V, Mat] = ZAkkaSource(source)
  }

  def apply[R, E, V, Mat](make: ZIO[R, E, Source[V, Mat]]): ZAkkaSource[R, E, V, Mat] = new ZAkkaSource(make)
  def apply[V, Mat](source: => Source[V, Mat]): ZAkkaSource[Any, Nothing, V, Mat] =
    new ZAkkaSource(ZIO.succeed(source))

  def from[R, E, V, Mat](make: ZIO[R, E, Source[V, Mat]]): ZAkkaSource[R, E, V, Mat] = new ZAkkaSource(make)
  def from[V, Mat](source: => Source[V, Mat]): ZAkkaSource[Any, Nothing, V, Mat] = new ZAkkaSource(ZIO.succeed(source))

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

final class ZAkkaSource[-R, +E, +V, +Mat] private (val make: ZIO[R, E, Source[V, Mat]]) {
  def mapAsync[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out]): ZAkkaSource[R1, E, Out, Mat] = {
    new ZAkkaSource(
      for {
        source <- make
        flow <- ZAkkaFlow[V].mapAsync(parallelism)(runTask).make
      } yield source.via(flow)
    )
  }

  def mapAsyncUnordered[R1 <: R, Out](parallelism: Int)(runTask: V => RIO[R1, Out]): ZAkkaSource[R1, E, Out, Mat] = {
    new ZAkkaSource(
      for {
        source <- make
        flow <- ZAkkaFlow[V].mapAsyncUnordered(parallelism)(runTask).make
      } yield source.via(flow)
    )
  }

  def interruptibleMapAsync[R1 <: R, Out](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: V => RIO[R1, Out]): ZAkkaSource[R1 with AkkaEnv, E, Out, Mat] = {
    new ZAkkaSource[R1 with AkkaEnv, E, Out, Mat](
      for {
        source <- make
        flow <- ZAkkaFlow[V].interruptibleMapAsync(parallelism, attributes)(runTask).make
      } yield source.via(flow)
    )
  }

  def interruptibleMapAsyncUnordered[R1 <: R, Out](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: V => RIO[R1, Out]): ZAkkaSource[R1 with AkkaEnv, E, Out, Mat] = {
    new ZAkkaSource[R1 with AkkaEnv, E, Out, Mat](
      for {
        source <- make
        flow <- ZAkkaFlow[V].interruptibleMapAsyncUnordered(parallelism, attributes)(runTask).make
      } yield source.via(flow)
    )
  }

  def via[Out](flow: => Graph[FlowShape[V, Out], Any]): ZAkkaSource[R, E, Out, Mat] = {
    viaMat(flow)(Keep.left)
  }

  def viaBuilder[Out](makeFlow: Flow[V @uncheckedVariance, V, NotUsed] => Graph[FlowShape[V, Out], Any])
    : ZAkkaSource[R, E, Out, Mat] = {
    viaBuilderMat(makeFlow)(Keep.left)
  }

  def viaMat[Out, Mat2, Mat3](flow: => Graph[FlowShape[V, Out], Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaSource[R, E, Out, Mat3] = {
    viaBuilderMat(_ => flow)(combine)
  }

  def viaBuilderMat[Out, Mat2, Mat3](makeFlow: Flow[V @uncheckedVariance, V, NotUsed] => Graph[
    FlowShape[V, Out],
    Mat2
  ])(
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

  def viaBuilderM[R1 <: R, E1 >: E, Out](makeFlow: Flow[V @uncheckedVariance, V, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[V, Out], Any]
  ]): ZAkkaSource[R1, E1, Out, Mat] = {
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

  def viaBuilderMatM[R1 <: R, E1 >: E, Out, Mat2, Mat3](makeFlow: Flow[V @uncheckedVariance, V, NotUsed] => ZIO[
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
