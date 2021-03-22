package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.{AkkaGrpcClient, StreamResponseRequestBuilder}
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.{UIO, URIO, URLayer, ZIO}

object DstreamClient {
  trait Service[Assignment, Result] {
    def requestBuilder(settings: GrpcClientSettings)
      : UIO[Int => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]]
  }

  final class DstreamClientApiBuilder[Assignment, Result] {
    def apply[R, Client <: AkkaGrpcClient](
      makeClient: GrpcClientSettings => URIO[R, Client]
    )(makeRequest: (Client, Int) => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment])(implicit
      t1: zio.Tag[Assignment],
      t2: zio.Tag[Result]
    ): URLayer[AkkaEnv with R, DstreamClient[Assignment, Result]] = {
      val effect: ZIO[R with AkkaEnv, Nothing, Service[Assignment, Result]] = for {
        env <- ZIO.environment[R]
      } yield {
        new Service[Assignment, Result] {
          override def requestBuilder(settings: GrpcClientSettings)
            : UIO[Int => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]] = {
            makeClient(settings)
              .provide(env)
              .map(client => workerId => makeRequest(client, workerId))
          }
        }
      }

      effect.toLayer
    }
  }

  def live[Assignment, Result] = new DstreamClientApiBuilder[Assignment, Result]
}
