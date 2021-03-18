package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.{AkkaGrpcClient, StreamResponseRequestBuilder}
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import zio.{UIO, URIO, URLayer, ZIO, ZLayer}

import scala.annotation.nowarn

object DstreamClientApi {
  final case class DstreamClientApiConfig(
    serverHost: NonEmptyString,
    serverPort: PortNumber,
    withTls: Boolean
  )

  trait Service[Assignment, Result] {
    def requestBuilder: UIO[() => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]]
  }

  final class DstreamClientApiBuilder[Assignment, Result](config: DstreamClientApiConfig) {
    def apply[R, Client <: AkkaGrpcClient](
      makeClient: GrpcClientSettings => URIO[R, Client]
    )(makeRequest: Client => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment])(implicit
      @nowarn("cat=unused") t1: zio.Tag[Assignment],
      @nowarn("cat=unused") t2: zio.Tag[Result]
    ): URLayer[AkkaEnv with R, DstreamClientApi[Assignment, Result]] = {
      val effect: ZIO[R with AkkaEnv, Nothing, Service[Assignment, Result]] = for {
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        clientSettings <- ZIO.effectTotal {
          import akkaSvc.actorSystem
          GrpcClientSettings
            .connectToServiceAt(config.serverHost, config.serverPort)
            .withTls(config.withTls)
            .withBackend("akka-http")
        }
        env <- ZIO.environment[R]
//        client = makeClient(clientSettings).provide(env)
      } yield {
        new Service[Assignment, Result] {
          override def requestBuilder: UIO[() => StreamResponseRequestBuilder[Source[Result, NotUsed], Assignment]] = {
            makeClient(clientSettings)
              .provide(env)
              .map(client => () => makeRequest(client))
          }
        }
      }

      ZLayer.fromEffect(effect)
    }
  }

  def live[Assignment, Result](config: DstreamClientApiConfig) =
    new DstreamClientApiBuilder[Assignment, Result](config)
}
