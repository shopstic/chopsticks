package dev.chopsticks.fp.iz_logging

import com.typesafe.config.{Config => LbConfig}
import dev.chopsticks.util.config.PureconfigLoader
import izumi.logstage.api.Log.{CustomContext, Level}
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.api.rendering.logunits.{Extractor, Styler}
import izumi.logstage.api.rendering.{logunits, RenderingOptions, StringRenderingPolicy}
import izumi.logstage.sink.file.models.{FileRotation, FileSinkConfig}
import izumi.logstage.sink.file.{FileServiceImpl, FileSink}
import izumi.logstage.sink.slf4j.LogSinkLegacySlf4jImpl
import logstage.{IzLogger, LogBIO3, LogstageZIO}
import pureconfig.ConfigReader
import zio.{Layer, ZIO, ZLayer}

object IzLogging {
  final case class JsonFileSinkConfig(
    path: String,
    rotationMaxFileCount: Int,
    rotationMaxFileBytes: Int
  )

  final case class IzLoggingConfig(level: Level, jsonFileSink: Option[JsonFileSinkConfig])

  object IzLoggingConfig {
    import dev.chopsticks.util.config.PureconfigConverters._
    implicit val levelConfigReader: ConfigReader[Level] =
      ConfigReader.fromString(l => Right(Level.parseSafe(l, Level.Info)))
    //noinspection TypeAnnotation
    implicit val configReader = ConfigReader[IzLoggingConfig]
  }

  trait Service {
    def logger: IzLogger
    def zioLogger: LogBIO3[ZIO]
  }

  final case class LiveService(logger: IzLogger, zioLogger: LogBIO3[ZIO]) extends Service

  def live(lbConfig: LbConfig, namespace: String = "iz-logging"): Layer[Nothing, IzLogging] = {
    live(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace))
  }

  def live(config: IzLoggingConfig): Layer[Nothing, IzLogging] = {
    ZLayer.succeed {
      val slf4jSink = new LogSinkLegacySlf4jImpl(
        new StringRenderingPolicy(
          RenderingOptions.simple,
          Some(
            new logunits.Renderer.Aggregate(
              Seq(
                new Extractor.SourcePosition(),
                Extractor.Space,
                new Styler.TrailingSpace(Seq(new Extractor.LoggerContext())),
                new Extractor.Message()
              )
            )
          )
        )
      )

      val maybeFileSink = config.jsonFileSink.map { fileSinkConfig =>
        object jsonFileSink
            extends FileSink(
              LogstageCirceRenderingPolicy(prettyPrint = false),
              new FileServiceImpl(fileSinkConfig.path),
              FileRotation.FileLimiterRotation(fileSinkConfig.rotationMaxFileCount),
              FileSinkConfig.soft(fileSinkConfig.rotationMaxFileBytes)
            ) {
          override def recoverOnFail(e: String): Unit = Console.err.println(e)
        }

        jsonFileSink
      }

      val logger = IzLogger(config.level, sinks = slf4jSink :: maybeFileSink.map(List(_)).getOrElse(Nil))
      val zioLogger = LogstageZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

      LiveService(logger, zioLogger)
    }
  }
}
