package dev.chopsticks.stream

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Source}
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.TaskExtensions
import zio.{IO, NeedsEnv, RIO, ZIO}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ExecutionContextExecutor, Future}

object ZAkkaFlow {
  implicit final class FlowToZAkkaFlow[-In, +Out, +Mat](flow: => Flow[In, Out, Mat]) {
    def toZAkkaFlow: UAkkaFlow[In, Out, Mat] = {
      new ZAkkaFlow(_ => ZIO.succeed(flow))
    }
  }

  def apply[In]: UAkkaFlow[In, In, NotUsed] = new ZAkkaFlow(_ => ZIO.succeed(Flow[In]))
}

final class ZAkkaFlow[-R, +E, -In, +Out, +Mat](val make: ZAkkaScope => ZIO[
  R,
  E,
  Flow[In, Out, Mat]
]) {
  def provide(r: R)(implicit ev: NeedsEnv[R]): IO[E, ZAkkaFlow[Any, E, In, Out, Mat]] = {
    toZIO.provide(r)
  }

  def toZIO: ZIO[R, E, ZAkkaFlow[Any, E, In, Out, Mat]] = {
    ZRunnable(make).toZIO.map(new ZAkkaFlow(_))
  }

  private def mapAsync_[R1 <: R, Next](runTask: (Out, ZAkkaScope) => RIO[R1, Next])(createFlow: (
    Flow[In @uncheckedVariance, Out @uncheckedVariance, Mat],
    Out @uncheckedVariance => Future[Next]
  ) => Flow[In @uncheckedVariance, Next, Mat @uncheckedVariance]): ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        runtime <- ZIO.runtime[R1 with AkkaEnv]
        promise <- zio.Promise.make[Nothing, Unit]
      } yield {
        implicit val rt: zio.Runtime[R1 with AkkaEnv] = runtime
        implicit val ec: ExecutionContextExecutor = rt.environment.get[AkkaEnv.Service].dispatcher

        createFlow(
          flow,
          item => {
            val task = for {
              fib <- scope.fork(runTask(item, scope))
              interruptFib <- scope.fork(promise.await *> fib.interrupt)
              ret <- fib.join
              _ <- interruptFib.interrupt
            } yield ret

            task.unsafeRunToFuture
          }
        )
          .watchTermination() { (mat, future) =>
            future.onComplete(_ => rt.unsafeRun(promise.succeed(())))
            mat
          }
      }
    })
  }

  private def scanAsync_[R1 <: R, S](zero: S)(runTask: (S, Out, ZAkkaScope) => RIO[R1, S])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, S, Mat] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        runtime <- ZIO.runtime[R1 with AkkaEnv]
        promise <- zio.Promise.make[Nothing, Unit]
      } yield {
        implicit val rt: zio.Runtime[R1 with AkkaEnv] = runtime
        implicit val ec: ExecutionContextExecutor = rt.environment.get[AkkaEnv.Service].dispatcher

        flow
          .scanAsync(zero) { (state, item) =>
            val task = for {
              fib <- scope.fork(runTask(state, item, scope))
              interruptFib <- scope.fork(promise.await *> fib.interrupt)
              ret <- fib.join
              _ <- interruptFib.interrupt
            } yield ret

            task.unsafeRunToFuture
          }
          .watchTermination() { (mat, future) =>
            future.onComplete(_ => rt.unsafeRun(promise.succeed(())))
            mat
          }
      }
    })
  }

  def scanAsync[R1 <: R, S](zero: S)(runTask: (S, Out) => RIO[R1, S]): ZAkkaFlow[R1 with AkkaEnv, E, In, S, Mat] = {
    scanAsync_(zero)((state, item, _) => runTask(state, item))
  }

  def scanAsyncWithScope[R1 <: R, S](zero: S)(runTask: (S, Out, ZAkkaScope) => RIO[R1, S])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, S, Mat] = {
    scanAsync_(zero)((state, item, scope) => runTask(state, item, scope))
  }

  def mapAsync[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    mapAsync_((item, _) => runTask(item)) { (flow, runFuture) =>
      flow
        .mapAsync(parallelism)(runFuture)
    }
  }

  def mapAsyncWithScope[R1 <: R, Next](parallelism: Int)(runTask: (Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    mapAsync_(runTask) { (flow, runFuture) =>
      flow
        .mapAsync(parallelism)(runFuture)
    }
  }

  def mapAsyncUnordered[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    mapAsync_((item, _) => runTask(item)) { (flow, runFuture) =>
      flow
        .mapAsyncUnordered(parallelism)(runFuture)
    }
  }
  def mapAsyncUnorderedWithScope[R1 <: R, Next](parallelism: Int)(runTask: (Out, ZAkkaScope) => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    mapAsync_(runTask) { (flow, runFuture) =>
      flow
        .mapAsyncUnordered(parallelism)(runFuture)
    }
  }

  def switchFlatMapConcat[R1 <: R, Next](f: Out => RIO[R1, Graph[SourceShape[Next], Any]])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        runtime <- ZIO.runtime[R1 with AkkaEnv]
      } yield {
        implicit val rt: zio.Runtime[R1 with AkkaEnv] = runtime

        val env = rt.environment
        val akkaService = env.get[AkkaEnv.Service]
        import akkaService.actorSystem

        flow
          .statefulMapConcat(() => {
            var currentKillSwitch = Option.empty[KillSwitch]

            in => {
              currentKillSwitch.foreach(_.shutdown())

              val (ks, s) = Source
                .fromGraph(rt.unsafeRun(f(in)))
                .viaMat(KillSwitches.single)(Keep.right)
                .preMaterialize()

              currentKillSwitch = Some(ks)
              List(s)
            }
          })
          .async
          .flatMapConcat(identity)
      }
    })
  }

  def viaZAkkaFlow[R1 <: R, E1 >: E, Next, Mat2, Mat3](next: ZAkkaFlow[R1, E1, Out @uncheckedVariance, Next, Mat2])
    : ZAkkaFlow[R1, E1, In, Next, Mat] = {
    viaZAkkaFlowMat(next)(Keep.left)
  }

  def viaZAkkaFlowMat[R1 <: R, E1 >: E, Next, Mat2, Mat3](next: ZAkkaFlow[R1, E1, Out @uncheckedVariance, Next, Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        nextFlow <- next.make(scope)
      } yield {
        flow
          .viaMat(nextFlow)(combine)
      }
    })
  }

  def via[Next](flow: => Graph[FlowShape[Out, Next], Any]): ZAkkaFlow[R, E, In, Next, Mat] = {
    viaMat(flow)(Keep.left)
  }

  def viaBuilder[Next](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => Graph[FlowShape[Out, Next], Any])
    : ZAkkaFlow[R, E, In, Next, Mat] = {
    viaBuilderMat(makeFlow)(Keep.left)
  }

  def viaMat[Next, Mat2, Mat3](flow: => Graph[FlowShape[Out, Next], Mat2])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R, E, In, Next, Mat3] = {
    viaBuilderMat(_ => flow)(combine)
  }

  def viaBuilderMat[Next, Mat2, Mat3](makeNext: Flow[Out @uncheckedVariance, Out, NotUsed] => Graph[
    FlowShape[Out, Next],
    Mat2
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R, E, In, Next, Mat3] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
      } yield {
        flow
          .viaMat(makeNext(Flow[Out]))(combine)
      }
    })
  }

  def viaM[R1 <: R, E1 >: E, Next](makeFlow: ZIO[R1, E1, Graph[FlowShape[Out, Next], Any]])
    : ZAkkaFlow[R1, E1, In, Next, Mat] = {
    viaMatM(makeFlow)(Keep.left)
  }

  def viaBuilderM[R1 <: R, E1 >: E, Next](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Any]
  ]): ZAkkaFlow[R1, E1, In, Next, Mat] = {
    viaBuilderMatWithScopeM((f, _) => makeFlow(f))(Keep.left)
  }

  def viaBuilderMatWithScopeM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: (
    Flow[Out @uncheckedVariance, Out, NotUsed],
    ZAkkaScope
  ) => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Mat2]
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        viaFlow <- makeFlow(Flow[Out], scope)
      } yield {
        flow
          .viaMat(viaFlow)(combine)
      }
    })
  }

  def viaMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeNext: ZIO[R1, E1, Graph[FlowShape[Out, Next], Mat2]])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    viaBuilderMatWithScopeM((_, _) => makeNext)(combine)
  }

  def viaBuilderMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => ZIO[
    R1,
    E1,
    Graph[FlowShape[Out, Next], Mat2]
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    viaBuilderMatWithScopeM((f, _) => makeFlow(f))(combine)
  }

  @deprecated("Use .killSwitch instead", since = "3.4.0")
  def interruptible: ZAkkaFlow[R, E, In, Out, UniqueKillSwitch] = killSwitch

  def killSwitch: ZAkkaFlow[R, E, In, Out, UniqueKillSwitch] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
      } yield {
        flow
          .viaMat(KillSwitches.single)(Keep.right)
      }
    })
  }
}
