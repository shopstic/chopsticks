package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.grpc.scaladsl.{PekkoGrpcClient, StreamResponseRequestBuilder}
import org.apache.pekko.stream.scaladsl.Source
import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio.{UIO, URIO, URLayer, ZIO, ZLayer}

trait DstreamClient[Assignment, Result] {
  def requestBuilder(settings: GrpcClientSettings)
    : UIO[Int => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]]
}

object DstreamClient {
  final class DstreamClientApiBuilder[Assignment, Result] {
    def apply[R, Client <: PekkoGrpcClient](
      makeClient: GrpcClientSettings => URIO[R, Client]
    )(makeRequest: (Client, Int) => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment])(implicit
      t1: zio.Tag[Assignment],
      t2: zio.Tag[Result]
    ): URLayer[PekkoEnv with R, DstreamClient[Assignment, Result]] = {
      val effect: ZIO[R with PekkoEnv, Nothing, DstreamClient[Assignment, Result]] = for {
        env <- ZIO.environment[R]
      } yield {
        new DstreamClient[Assignment, Result] {
          override def requestBuilder(settings: GrpcClientSettings)
            : UIO[Int => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]] = {
            makeClient(settings)
              .provideEnvironment(env)
              .map(client => workerId => makeRequest(client, workerId))
          }
        }
      }

      ZLayer(effect)
    }
  }

  def live[Assignment, Result] = new DstreamClientApiBuilder[Assignment, Result]
}
