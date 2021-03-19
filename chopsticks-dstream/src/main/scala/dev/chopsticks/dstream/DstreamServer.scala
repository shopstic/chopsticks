package dev.chopsticks.dstream

import akka.http.scaladsl.Http
import akka.http.scaladsl.settings.ServerSettings
import akka.util.Timeout
import dev.chopsticks.fp.ZManageable
import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import zio.{RManaged, Task, URLayer, ZManaged}

import scala.concurrent.duration._

object DstreamServer {
  final case class DstreamServerConfig(
    port: PortNumber,
    interface: NonEmptyString = "0.0.0.0",
    shutdownTimeout: Timeout = 10.seconds
  )

  trait Service[Assignment, Result] {
    def manage(config: DstreamServerConfig)
      : RManaged[DstreamServerHandler[Assignment, Result] with AkkaEnv, Http.ServerBinding]
  }

  def manage[Assignment: zio.Tag, Result: zio.Tag](
    config: DstreamServerConfig
  ): RManaged[DstreamServerHandler[Assignment, Result] with AkkaEnv, Http.ServerBinding] = {
    for {
      akkaSvc <- ZManaged.access[AkkaEnv](_.get)
      handler <- ZManaged.access[DstreamServerHandler[Assignment, Result]](_.get)
      binding <- ZManaged
        .make {
          for {
            fn <- handler.create
            binding <- Task
              .fromFuture { _ =>
                import akkaSvc.actorSystem
                val settings = ServerSettings(actorSystem)

                Http()
                  .newServerAt(interface = config.interface, port = config.port)
                  .withSettings(
                    settings
                      .withPreviewServerSettings(settings.previewServerSettings.withEnableHttp2(true))
                  )
                  .bind(fn)
              }
          } yield binding
        } { binding =>
          Task
            .fromFuture(_ => binding.terminate(config.shutdownTimeout.duration))
            .orDie
        }
    } yield binding
  }

  def live[Assignment: zio.Tag, Result: zio.Tag]
    : URLayer[DstreamServerHandler[Assignment, Result] with AkkaEnv, DstreamServer[Assignment, Result]] = {
    ZManageable(manage[Assignment, Result] _)
      .toLayer(fn => (config: DstreamServerConfig) => fn(config))
  }
}
