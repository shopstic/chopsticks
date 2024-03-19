package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.scaladsl.{ServerReflection, ServiceHandler}
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import dev.chopsticks.dstream.DstreamServerHandlerFactory.DstreamServerPartialHandler
import dev.chopsticks.fp.pekko_env.PekkoEnv
import grpc.health.v1.{Health, HealthHandler}
import zio.{UIO, URLayer, Unsafe, ZIO, ZLayer}

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait DstreamServerHandler[Assignment, Result] {
  def create: UIO[HttpRequest => Future[HttpResponse]]
}

object DstreamServerHandler {

  def live[Assignment: zio.Tag, Result: zio.Tag]: URLayer[
    DstreamServerHandlerFactory[Assignment, Result] with DstreamState[Assignment, Result] with PekkoEnv,
    DstreamServerHandler[Assignment, Result]
  ] = {
    val effect: ZIO[
      DstreamServerHandlerFactory[Assignment, Result] with DstreamState[Assignment, Result] with PekkoEnv,
      Nothing,
      DstreamServerHandler[Assignment, Result]
    ] = for {
      pekkoSvc <- ZIO.service[PekkoEnv]
      pekkoRuntime <- ZIO.runtime[PekkoEnv]
      stateSvc <- ZIO.service[DstreamState[Assignment, Result]]
      handlerFactory <- ZIO.service[DstreamServerHandlerFactory[Assignment, Result]]
    } yield {
      new DstreamServerHandler[Assignment, Result] {
        override def create: UIO[HttpRequest => Future[HttpResponse]] = {
          handlerFactory
            .create { (in, metadata) =>
              import pekkoSvc.{actorSystem, dispatcher}

              val (ks, resultSource) = in
                .viaMat(KillSwitches.single)(Keep.right)
                .preMaterialize()

              val assignmentFutureSource = Unsafe.unsafe { implicit unsafe =>
                pekkoRuntime.unsafe.runToFuture(stateSvc.enqueueWorker(resultSource, metadata))
              }

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
            .map { case DstreamServerPartialHandler(handler, serviceDescription) =>
              import pekkoSvc.actorSystem

              ServiceHandler.concatOrNotFound(
                handler,
                HealthHandler.partial(new DstreamHealthImpl),
                ServerReflection.partial(List(serviceDescription, Health))
              )
            }
        }
      }
    }

    ZLayer(effect)
  }
}
