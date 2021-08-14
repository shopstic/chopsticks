package dev.chopsticks.stream

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Source}
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.TaskExtensions
import zio.{Exit, RIO, ZIO, ZScope}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.ExecutionContextExecutor

object ZAkkaFlow {
  implicit final class FlowToZAkkaFlow[-In, +Out, +Mat](flow: => Flow[In, Out, Mat]) {
    def toZAkkaFlow: ZAkkaFlow[Any, Nothing, In, Out, Mat] = {
      new ZAkkaFlow(_ => ZIO.succeed(flow))
    }
  }

  def apply[In]: ZAkkaFlow[Any, Nothing, In, In, NotUsed] = new ZAkkaFlow(_ => ZIO.succeed(Flow[In]))
}

final class ZAkkaFlow[-R, +E, -In, +Out, +Mat] private (val make: ZScope[Exit[Any, Any]] => ZIO[
  R,
  E,
  Flow[In, Out, Mat]
]) {
  def requireEnv: ZIO[R, E, ZAkkaFlow[Any, E, In, Out, Mat]] = {
    ZRunnable(make).toZIO.map(new ZAkkaFlow(_))
  }

  def mapAsync[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        runtime <- ZIO.runtime[R1 with AkkaEnv]
        promise <- zio.Promise.make[Nothing, Unit]
      } yield {
        implicit val rt: zio.Runtime[R1 with AkkaEnv] = runtime
        implicit val ec: ExecutionContextExecutor = rt.environment.get[AkkaEnv.Service].dispatcher

        flow
          .mapAsync(parallelism) { a =>
            val task = for {
              fib <- runTask(a).forkIn(scope)
              interruptFib <- (promise.await *> fib.interrupt).forkIn(scope)
              ret <- fib.join
              _ <- interruptFib.interrupt
            } yield ret

            task.unsafeRunToFuture
          }
          .watchTermination() { (mat, f) =>
            f.onComplete(_ => rt.unsafeRun(promise.succeed(())))
            mat
          }
      }
    })
  }

  def mapAsyncUnordered[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        runtime <- ZIO.runtime[R1 with AkkaEnv]
        promise <- zio.Promise.make[Nothing, Unit]
      } yield {
        implicit val rt: zio.Runtime[R1 with AkkaEnv] = runtime
        implicit val ec: ExecutionContextExecutor = rt.environment.get[AkkaEnv.Service].dispatcher

        flow
          .mapAsyncUnordered(parallelism) { a =>
            val task = for {
              fib <- runTask(a).forkIn(scope)
              interruptFib <- (promise.await *> fib.interrupt).forkIn(scope)
              ret <- fib.join
              _ <- interruptFib.interrupt
            } yield ret

            task.unsafeRunToFuture
          }
          .watchTermination() { (mat, f) =>
            f.onComplete(_ => rt.unsafeRun(promise.succeed(())))
            mat
          }
      }
    })
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
    viaMatM(makeFlow(Flow[Out]))(Keep.left)
  }

  def viaMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeNext: ZIO[R1, E1, Graph[FlowShape[Out, Next], Mat2]])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    new ZAkkaFlow(scope => {
      for {
        flow <- make(scope)
        nextFlow <- makeNext
      } yield {
        flow
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
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    viaMatM(makeFlow(Flow[Out]))(combine)
  }

  def interruptible: ZAkkaFlow[R, E, In, Out, UniqueKillSwitch] = {
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
