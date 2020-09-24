package dev.chopsticks.fp.iz_logging

import com.typesafe.config.{Config => LbConfig}
import dev.chopsticks.fp.log_env.LogCtx
import dev.chopsticks.util.config.PureconfigLoader
import izumi.logstage.api.Log.{CustomContext, Level}
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.sink.QueueingSink
import izumi.logstage.sink.file.models.{FileRotation, FileSinkConfig}
import izumi.logstage.sink.file.{FileServiceImpl, FileSink}
import logstage.{ConsoleSink, IzLogger, LogBIO3, LogstageZIO, StaticLogRouter}
import pureconfig.ConfigReader
import zio.{Layer, UIO, ZIO, ZManaged}

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
    def ctxLogger(implicit logCtx: LogCtx): IzLogger
    def zioLogger: LogBIO3[ZIO]
    def ctxZioLogger(implicit logCtx: LogCtx): LogBIO3[ZIO]
  }

  final case class LiveService(logger: IzLogger, zioLogger: LogBIO3[ZIO]) extends Service {
    override def ctxLogger(implicit logCtx: LogCtx) = logger("ctx" -> logCtx.name)
    override def ctxZioLogger(implicit logCtx: LogCtx) = zioLogger("ctx" -> logCtx.name)
  }

  def live(lbConfig: LbConfig, namespace: String = "iz-logging"): Layer[Nothing, IzLogging] = {
    live(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace))
  }

  def live(config: IzLoggingConfig): Layer[Nothing, IzLogging] = {
    val managed: ZManaged[Any, Nothing, Service] = for {
      service <- ZManaged.make {
        UIO {
          val consoleSink = ConsoleSink.text(colored = true)

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

            new QueueingSink(jsonFileSink)
          }

          val logger = IzLogger(config.level, sinks = consoleSink :: maybeFileSink.toList)
          val zioLogger = LogstageZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

          maybeFileSink.foreach(_.start())

          // configure slf4j to use LogStage router
          StaticLogRouter.instance.setup(logger.router)
          LiveService(logger, zioLogger)
        }
      } { service =>
        UIO(service.logger.router.close())
      }
    } yield service
    managed.toLayer
  }
}
