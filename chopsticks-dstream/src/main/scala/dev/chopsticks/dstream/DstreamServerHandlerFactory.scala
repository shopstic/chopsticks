package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.ServiceDescription
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Source
import zio.{UIO, URIO, URLayer, ZManaged}

import scala.concurrent.Future

object DstreamServerHandlerFactory {
  final case class DstreamServerPartialHandler(
    handler: PartialFunction[HttpRequest, Future[HttpResponse]],
    serviceDescription: ServiceDescription
  )

  trait Service[Assignment, Result] {
    def create(handle: (Source[Result, NotUsed], Metadata) => Source[Assignment, NotUsed])
      : UIO[DstreamServerPartialHandler]
  }

  final class DstreamServerApiBuilder[Assignment, Result] {
    def apply[R](factory: ((
      Source[Result, NotUsed],
      Metadata
    ) => Source[Assignment, NotUsed]) => URIO[
      R,
      DstreamServerPartialHandler
    ])(implicit
      t1: zio.Tag[Assignment],
      t2: zio.Tag[Result]
    ): URLayer[R, DstreamServerHandlerFactory[Assignment, Result]] = {
      ZManaged
        .environment[R].map { env =>
          new Service[Assignment, Result] {
            override def create(handle: (Source[Result, NotUsed], Metadata) => Source[Assignment, NotUsed])
              : UIO[DstreamServerPartialHandler] = {
              factory(handle).provide(env)
            }
          }
        }
        .toLayer[Service[Assignment, Result]]
    }
  }

  def live[Assignment, Result] = new DstreamServerApiBuilder[Assignment, Result]
}
