package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{Attributes, KillSwitch, KillSwitches}
import dev.chopsticks.fp.ZService
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext.TaskExtensions
import zio.{Has, RIO, ZIO}

import scala.concurrent.Future

final class ZAkkaFlow[In] private {
  def interruptibleMapAsync[R <: Has[_], Out](
    parallelism: Int
  )(runTask: In => RIO[R, Out]): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService._

      Flow
        .lazyFutureFlow(() => {
          val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])

          Future.successful(
            Flow[In]
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
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed))
    }
  }

  def interruptibleMapAsyncUnordered[R <: Has[_], Out](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: In => RIO[R, Out]): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService._

      Flow
        .lazyFutureFlow(() => {
          val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])
          val interruption = completionPromise.await

          val flow = Flow[In]
            .mapAsyncUnordered(parallelism) { a =>
              val interruptibleTask = for {
                fib <- runTask(a).fork
                c <- (interruption *> fib.interrupt).fork
                ret <- fib.join
                _ <- c.interrupt
              } yield ret

              interruptibleTask.unsafeRunToFuture
            }

          val flowWithAttrs = attributes.fold(flow)(attrs => flow.withAttributes(attrs))

          Future.successful(
            flowWithAttrs
              .watchTermination() { (_, f) =>
                f.onComplete { _ =>
                  val _ = rt.unsafeRun(completionPromise.succeed(()))
                }
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed)(akkaService.dispatcher))
    }
  }

  def switchFlatMapConcat[R <: Has[_], Out](
    f: In => RIO[R, Source[Out, Any]]
  ): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { rt =>
      val env = rt.environment
      val akkaService = ZService.get[AkkaEnv.Service](env)
      import akkaService.actorSystem

      Flow[In]
        .statefulMapConcat(() => {
          var currentKillSwitch = Option.empty[KillSwitch]

          in => {
            currentKillSwitch.foreach(_.shutdown())

            val (ks, s) = rt
              .unsafeRun(f(in))
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

  def mapAsync[R <: Has[_], Out](
    parallelism: Int
  )(runTask: In => RIO[R, Out]): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      Flow[In]
        .mapAsync(parallelism) { a => runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten }
    }
  }

  def mapAsyncUnordered[R <: Has[_], Out](
    parallelism: Int
  )(runTask: In => RIO[R, Out]): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      Flow[In]
        .mapAsyncUnordered(parallelism) { a =>
          runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
        }
    }
  }
}

object ZAkkaFlow {
  def apply[In]: ZAkkaFlow[In] = new ZAkkaFlow[In]()
}
