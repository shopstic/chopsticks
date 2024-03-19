package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.ServiceDescription
import org.apache.pekko.grpc.scaladsl.Metadata
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.stream.scaladsl.Source
import zio.{UIO, URIO, URLayer, ZIO, ZLayer}

import scala.concurrent.Future

trait DstreamServerHandlerFactory[Assignment, Result] {
  def create(handle: (Source[Result, NotUsed], Metadata) => Source[Assignment, NotUsed])
    : UIO[DstreamServerHandlerFactory.DstreamServerPartialHandler]
}

object DstreamServerHandlerFactory {
  final case class DstreamServerPartialHandler(
    handler: PartialFunction[HttpRequest, Future[HttpResponse]],
    serviceDescription: ServiceDescription
  )

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
      val effect = ZIO
        .environment[R].map { env =>
          new DstreamServerHandlerFactory[Assignment, Result] {
            override def create(handle: (Source[Result, NotUsed], Metadata) => Source[Assignment, NotUsed])
              : UIO[DstreamServerPartialHandler] = {
              factory(handle).provideEnvironment(env)
            }
          }
        }

      ZLayer(effect)
    }
  }

  def live[Assignment, Result] = new DstreamServerApiBuilder[Assignment, Result]
}
