package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Graph, KillSwitch, KillSwitches, SourceShape, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, Keep, Source}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.TaskExtensions
import zio.{RIO, ZIO}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future

object ZAkkaFlow {
  implicit final class FlowToZAkkaFlow[-In, +Out, +Mat](flow: => Flow[In, Out, Mat]) {
    def toZAkkaFlow: ZAkkaFlow[Any, Nothing, In, Out, Mat] = ZAkkaFlow(flow)
  }

  def apply[In]: ZAkkaFlow[Any, Nothing, In, In, NotUsed] = {
    new ZAkkaFlow(ZIO.succeed(Flow[In]))
  }

  def apply[R, E, In, Out, Mat](make: ZIO[R, E, Flow[In, Out, Mat]]): ZAkkaFlow[R, E, In, Out, Mat] =
    new ZAkkaFlow(make)

  def apply[In, Out, Mat](flow: => Flow[In, Out, Mat]): ZAkkaFlow[Any, Nothing, In, Out, Mat] =
    new ZAkkaFlow(ZIO.succeed(flow))
}

final class ZAkkaFlow[-R, +E, -In, +Out, +Mat] private (val make: ZIO[R, E, Flow[In, Out, Mat]]) {
  def mapAsync[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next]): ZAkkaFlow[R1, E, In, Next, Mat] = {
    new ZAkkaFlow(
      make
        .flatMap { flow =>
          ZIO.runtime[R1].map { implicit rt =>
            flow
              .mapAsync(parallelism) { a =>
                runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
              }
          }
        }
    )
  }

  def mapAsyncUnordered[R1 <: R, Next](parallelism: Int)(runTask: Out => RIO[R1, Next])
    : ZAkkaFlow[R1, E, In, Next, Mat] = {
    new ZAkkaFlow(
      make
        .flatMap { flow =>
          ZIO.runtime[R1].map { implicit rt =>
            flow
              .mapAsync(parallelism) { a =>
                runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
              }
          }
        }
    )
  }

  private def interruptibleMapAsync_[R1 <: R, Next](
    runTask: Out => RIO[R1, Next],
    attributes: Option[Attributes]
  )(
    createFlow: (Out @uncheckedVariance => Future[Next]) => Flow[Out, Next, NotUsed]
  ): ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(
      make
        .flatMap { flow =>
          ZIO.runtime[R1 with AkkaEnv].map { implicit rt =>
            val env = rt.environment
            val akkaService = env.get[AkkaEnv.Service]
            import akkaService._

            val nextFlow = Flow
              .lazyFutureFlow(() => {
                val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])
                val underlyingFlow = createFlow { a: Out =>
                  val interruptibleTask = for {
                    fib <- runTask(a).fork
                    c <- (completionPromise.await *> fib.interrupt).fork
                    ret <- fib.join
                    _ <- c.interrupt
                  } yield ret

                  interruptibleTask.unsafeRunToFuture
                }

                val underlyingFlowWithAttrs =
                  attributes.fold(underlyingFlow)(attrs => underlyingFlow.withAttributes(attrs))

                Future
                  .successful(
                    underlyingFlowWithAttrs
                      .watchTermination() { (_, f) =>
                        f.onComplete { _ =>
                          val _ = rt.unsafeRun(completionPromise.succeed(()))
                        }
                        NotUsed
                      }
                  )
              })

            flow.via(nextFlow)
          }
        }
    )
  }

  def interruptibleMapAsync[R1 <: R, Next](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: Out => RIO[R1, Next]): ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    interruptibleMapAsync_(runTask, attributes) { runFuture =>
      Flow[Out].mapAsync(parallelism)(runFuture)
    }
  }

  def interruptibleMapAsyncUnordered[R1 <: R, Next](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: Out => RIO[R1, Next]): ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    interruptibleMapAsync_(runTask, attributes) { runFuture =>
      Flow[Out].mapAsyncUnordered(parallelism)(runFuture)
    }
  }

  def switchFlatMapConcat[R1 <: R, Next](f: Out => RIO[R1, Graph[SourceShape[Next], Any]])
    : ZAkkaFlow[R1 with AkkaEnv, E, In, Next, Mat] = {
    new ZAkkaFlow(
      make
        .flatMap { flow =>
          ZIO.runtime[R1 with AkkaEnv].map { implicit rt =>
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
        }
    )
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

  def viaBuilderMat[Next, Mat2, Mat3](makeFlow: Flow[Out @uncheckedVariance, Out, NotUsed] => Graph[
    FlowShape[Out, Next],
    Mat2
  ])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R, E, In, Next, Mat3] = {
    new ZAkkaFlow(
      make
        .map { source =>
          source.viaMat(makeFlow(Flow[Out]))(combine)
        }
    )
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

  def viaMatM[R1 <: R, E1 >: E, Next, Mat2, Mat3](makeFlow: ZIO[R1, E1, Graph[FlowShape[Out, Next], Mat2]])(
    combine: (Mat, Mat2) => Mat3
  ): ZAkkaFlow[R1, E1, In, Next, Mat3] = {
    new ZAkkaFlow(
      for {
        source <- make
        flow <- makeFlow
      } yield {
        source.viaMat(flow)(combine)
      }
    )
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
    new ZAkkaFlow(
      make.map(_.viaMat(KillSwitches.single)(Keep.right))
    )
  }
}
