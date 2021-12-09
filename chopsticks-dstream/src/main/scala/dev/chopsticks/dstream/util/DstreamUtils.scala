package dev.chopsticks.dstream.util

import akka.grpc.GrpcClientSettings
import dev.chopsticks.dstream.DstreamWorker.DstreamWorkerConfig
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.{HoconConfig, TypedConfig}
import dev.chopsticks.fp.iz_logging.IzLogging
import izumi.logstage.api.Log
import pureconfig.ConfigReader
import pureconfig.error.ExceptionThrown
import zio.{RLayer, URIO}

import scala.util.Try

object DstreamUtils {
  def grpcClientSettingsConfigReader: URIO[AkkaEnv, ConfigReader[GrpcClientSettings]] =
    AkkaEnv.actorSystem.map { implicit as =>
      ConfigReader.configConfigReader.emap { config =>
        Try {
          GrpcClientSettings
            .fromConfig(
              config
                .withFallback(as.settings.config
                  .getConfig("akka.grpc.client")
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
  ): RLayer[IzLogging with HoconConfig with AkkaEnv, TypedConfig[DstreamWorkerConfig]] = {
    grpcClientSettingsConfigReader
      .toManaged_
      .flatMap { implicit settingsConfigReader: ConfigReader[GrpcClientSettings] =>
        // noinspection TypeAnnotation
        implicit val configReader = {
          import dev.chopsticks.util.config.PureconfigConverters._
          ConfigReader[DstreamWorkerConfig]
        }
        TypedConfig.live[DstreamWorkerConfig](configNamespace, logLevel).build
      }
      .toLayerMany
  }
}
