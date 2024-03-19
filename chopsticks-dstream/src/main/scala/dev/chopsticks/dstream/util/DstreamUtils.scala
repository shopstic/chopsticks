package dev.chopsticks.dstream.util

import org.apache.pekko.grpc.GrpcClientSettings
import dev.chopsticks.dstream.DstreamWorker.DstreamWorkerConfig
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.{HoconConfig, TypedConfig}
import dev.chopsticks.fp.iz_logging.IzLogging
import izumi.logstage.api.Log
import pureconfig.ConfigReader
import pureconfig.error.ExceptionThrown
import zio.{RLayer, URIO, ZLayer}

import scala.util.Try

object DstreamUtils {
  def grpcClientSettingsConfigReader: URIO[PekkoEnv, ConfigReader[GrpcClientSettings]] =
    PekkoEnv.actorSystem.map { implicit as =>
      ConfigReader.configConfigReader.emap { config =>
        Try {
          GrpcClientSettings
            .fromConfig(
              config
                .withFallback(as.settings.config
                  .getConfig("pekko.grpc.client")
                  .getConfig("\"*\""))
            )
        }.fold(
          e => Left(ExceptionThrown(e)),
          Right(_)
        )
      }
    }

  def liveWorkerTypedConfig(
    configNamespace: String = "app",
    logLevel: Log.Level = Log.Level.Info
  ): RLayer[IzLogging with HoconConfig with PekkoEnv, TypedConfig[DstreamWorkerConfig]] = {
    val effect = grpcClientSettingsConfigReader
      .flatMap { implicit settingsConfigReader: ConfigReader[GrpcClientSettings] =>
        //noinspection TypeAnnotation
        implicit val configReader = {
          import dev.chopsticks.util.config.PureconfigConverters._
          ConfigReader[DstreamWorkerConfig]
        }
        TypedConfig.live[DstreamWorkerConfig](configNamespace, logLevel).build.map(_.get)
      }

    ZLayer.scoped(effect)
  }
}
