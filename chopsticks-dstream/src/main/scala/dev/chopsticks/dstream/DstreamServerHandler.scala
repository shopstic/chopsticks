package dev.chopsticks.dstream

import akka.NotUsed
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Source}
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.{UIO, URLayer, ZIO, ZLayer}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object DstreamServerHandler {
  trait Service[Assignment, Result] {
    def create: UIO[HttpRequest => Future[HttpResponse]]
  }

  def live[Assignment: zio.Tag, Result: zio.Tag]: URLayer[
    DstreamServerHandlerFactory[Assignment, Result] with DstreamState[Assignment, Result] with AkkaEnv,
    DstreamServerHandler[Assignment, Result]
  ] = {
    val effect: ZIO[
      DstreamServerHandlerFactory[Assignment, Result] with DstreamState[Assignment, Result] with AkkaEnv,
      Nothing,
      Service[Assignment, Result]
    ] = for {
      akkaSvc <- ZIO.access[AkkaEnv](_.get)
      akkaRuntime <- ZIO.runtime[AkkaEnv]
      stateSvc <- ZIO.access[DstreamState[Assignment, Result]](_.get)
      handlerFactory <- ZIO.access[DstreamServerHandlerFactory[Assignment, Result]](_.get)
    } yield {
      new Service[Assignment, Result] {
        override def create: UIO[HttpRequest => Future[HttpResponse]] = {
          handlerFactory.create { (in, metadata) =>
            import akkaSvc.{actorSystem, dispatcher}

            val (ks, resultSource) = in
              .viaMat(KillSwitches.single)(Keep.right)
              .preMaterialize()

            val assignmentFutureSource = akkaRuntime.unsafeRunToFuture(stateSvc.enqueueWorker(resultSource, metadata))

            Source
              .futureSource(assignmentFutureSource)
              .watchTermination() {
                case (_, f) =>
                  f
                    .transformWith { result =>
                      assignmentFutureSource
                        .cancel()
                        .map(exit => result.flatMap(_ => exit.toEither.toTry))
                    }
                    .onComplete {
                      case Success(_) => ks.shutdown()
                      case Failure(ex) => ks.abort(ex)
                    }
                  NotUsed
              }
          }
        }
      }
    }

    ZLayer.fromEffect(effect)
  }
}
