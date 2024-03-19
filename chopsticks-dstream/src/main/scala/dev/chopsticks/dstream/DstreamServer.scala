package dev.chopsticks.dstream

import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.util.Timeout
import dev.chopsticks.fp.pekko_env.PekkoEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import zio.{RIO, Scope, URLayer, ZIO, ZLayer}

import scala.concurrent.duration._

trait DstreamServer[Assignment, Result] {
  def manage(config: DstreamServer.DstreamServerConfig)
    : RIO[DstreamServerHandler[Assignment, Result] with PekkoEnv with Scope, Http.ServerBinding]
}

object DstreamServer {
  final case class DstreamServerConfig(
    port: PortNumber,
    interface: NonEmptyString = "0.0.0.0",
    shutdownTimeout: Timeout = 10.seconds,
    idleTimeout: Option[Timeout] = None
  )

  def live[Assignment: zio.Tag, Result: zio.Tag]
    : URLayer[DstreamServerHandler[Assignment, Result] with PekkoEnv, DstreamServer[Assignment, Result]] = {
    val effect =
      for {
        akkaSvc <- ZIO.service[PekkoEnv]
        handler <- ZIO.service[DstreamServerHandler[Assignment, Result]]
      } yield new DstreamServer[Assignment, Result] {
        override def manage(config: DstreamServerConfig)
          : RIO[DstreamServerHandler[Assignment, Result] with PekkoEnv with Scope, Http.ServerBinding] = {
          ZIO
            .acquireRelease {
              for {
                fn <- handler.create
                binding <- ZIO
                  .fromFuture { _ =>
                    import akkaSvc.actorSystem
                    val settings = ServerSettings(actorSystem)

                    Http()
                      .newServerAt(interface = config.interface, port = config.port)
                      .withSettings(
                        settings
                          .withPreviewServerSettings(
                            settings
                              .withTimeouts(settings.timeouts.withIdleTimeout(
                                config.idleTimeout.map(_.duration).getOrElse(Duration.Inf)
                              ))
                              .previewServerSettings
                              .withEnableHttp2(true)
                          )
                      )
                      .bind(fn)
                  }
              } yield binding
            } { binding =>
              ZIO
                .fromFuture(_ => binding.terminate(config.shutdownTimeout.duration))
                .orDie
            }
        }
      }

    ZLayer(effect)
  }
}
