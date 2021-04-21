package dev.chopsticks.fp.iz_logging

import com.typesafe.config.Config
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.util.config.PureconfigLoader
import izumi.fundamentals.platform.time.IzTimeSafe
import izumi.logstage.api.Log
import izumi.logstage.api.Log.{CustomContext, Level}
import izumi.logstage.api.logger.LogSink
import izumi.logstage.api.rendering.RenderingPolicy
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.api.rendering.logunits.Styler.TrimType
import izumi.logstage.api.rendering.logunits.{Extractor, Renderer, Styler}
import izumi.logstage.sink.QueueingSink
import izumi.logstage.sink.file.models.{FileRotation, FileSinkConfig}
import izumi.logstage.sink.file.{FileServiceImpl, FileSink}
import logstage._
import pureconfig.ConfigReader
import zio._

object IzLogging {

  final case class JsonFileSinkConfig(
    path: String,
    rotationMaxFileCount: Int,
    rotationMaxFileBytes: Int
  )

  final case class IzLoggingConfig(level: Level, noColor: Boolean, jsonFileSink: Option[JsonFileSinkConfig])

  object IzLoggingConfig {
    import dev.chopsticks.util.config.PureconfigConverters._
    implicit val levelConfigReader: ConfigReader[Level] =
      ConfigReader.fromString(l => Right(Level.parseSafe(l, Level.Info)))
    //noinspection TypeAnnotation
    implicit val configReader = ConfigReader[IzLoggingConfig]
  }

  trait Service {
    def logger: IzLogger
    def loggerWithCtx(ctx: LogCtx): IzLogger
    def zioLogger: LogIO3[ZIO]
    def zioLoggerWithCtx(ctx: LogCtx): LogIO3[ZIO]
    def logLevel: Log.Level
  }

  final case class LiveService(logger: IzLogger, zioLogger: LogIO3[ZIO], logLevel: Log.Level) extends Service {
    override def loggerWithCtx(ctx: LogCtx): IzLogger =
      logger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
    override def zioLoggerWithCtx(ctx: LogCtx): LogIO3[ZIO] =
      zioLogger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
  }

  def logger: URIO[IzLogging, IzLogger] = ZIO.access[IzLogging](_.get.logger)
  def loggerWithContext(ctx: LogCtx): URIO[IzLogging, IzLogger] = ZIO.access[IzLogging](_.get.loggerWithCtx(ctx))
  def zioLogger: URIO[IzLogging, LogIO3[ZIO]] = ZIO.access[IzLogging](_.get.zioLogger)

  def unsafeCreate(config: IzLoggingConfig, routerFactory: IzLoggingRouter.Service): LiveService = {
    val consoleSink = {
      val renderingPolicy =
        if (config.noColor) RenderingPolicy.simplePolicy(Some(IzLogTemplates.consoleLayout))
        else RenderingPolicy.coloringPolicy(Some(IzLogTemplates.consoleLayout))
      ConsoleSink(renderingPolicy)
    }

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

    val sinks = consoleSink :: maybeFileSink.toList
    val logger =
      IzLogger(routerFactory.create(config.level, sinks))(IzLoggingCustomRenderers.LoggerTypeCtxKey -> "iz")
    val zioLogger = LogZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

    maybeFileSink.foreach(_.start())

    // configure slf4j to use LogStage router
    StaticLogRouter.instance.setup(logger.router)
    LiveService(logger, zioLogger, config.level)
  }

  def unsafeLoadConfig(hoconConfig: Config, configNamespace: String = "iz-logging"): IzLoggingConfig = {
    PureconfigLoader.unsafeLoad[IzLoggingConfig](hoconConfig, configNamespace)
  }

  def create(config: IzLoggingConfig): RIO[IzLoggingRouter, LiveService] = {
    ZIO
      .access[IzLoggingRouter](_.get)
      .flatMap { routerFactory =>
        Task {
          unsafeCreate(config, routerFactory)
        }
      }
  }

  def live(config: IzLoggingConfig): ZLayer[IzLoggingRouter, Throwable, IzLogging] = {
    val managed = ZManaged.make {
      create(config)
    } { service =>
      UIO(service.logger.router.close())
    }

    managed.toLayer
  }

  def live(hoconConfig: Config): RLayer[IzLoggingRouter, IzLogging] = {
    val effect = for {
      config <- Task(unsafeLoadConfig(hoconConfig))
      service <- create(config)
    } yield service

    effect.toLayer
  }

  def live(): ZLayer[HoconConfig with IzLoggingRouter, Throwable, IzLogging] = {
    val managed = for {
      hoconConfig <- HoconConfig.get.toManaged_
      service <- live(hoconConfig).build
    } yield service

    ZLayer.fromManagedMany(managed)
  }
}

object IzLogTemplates {
  import IzLoggingCustomRenderers._

  val consoleLayout = new Renderer.Aggregate(
    Seq(
      new IzLoggingCustomStylers.LevelColor(
        Seq(
          new Extractor.Constant("["),
          new Extractor.Level(1),
          new Extractor.Constant("]"),
          Extractor.Space,
          new Extractor.Constant("["),
          new Extractor.Timestamp(IzTimeSafe.ISO_LOCAL_DATE_TIME_3NANO),
          new Extractor.Constant("]")
        )
      ),
      Extractor.Space,
      new LocationRenderer(
        sourceExtractor = new ContextSourcePositionExtractor(new Extractor.SourcePosition()),
        fallbackRenderer = new LoggerName(42)
      ),
      Extractor.Space,
      new Extractor.Constant("["),
      new Extractor.ThreadId(),
      new Extractor.Constant(":"),
      new Styler.Trim(Seq(new Extractor.ThreadName()), 20, TrimType.Center, Some("…")),
      new Extractor.Constant("]"),
      Extractor.Space,
      new Styler.TrailingSpace(Seq(new FilteringContextExtractor(new LoggerContext()))),
      new MessageExtractor()
    )
  )
}

trait IzLoggingFilter {
  def exclude(logEntry: Log.Entry): Boolean
}

object IzLoggingSinks {
  final class IzFilteringSink(filters: Iterable[IzLoggingFilter], underlying: LogSink) extends LogSink {
    override def flush(e: Log.Entry): Unit = {
      if (filters.exists(f => f.exclude(e))) ()
      else underlying.flush(e)
    }

    override def close(): Unit = underlying.close()
  }

  object IzFilteringSink {
    def apply(filters: Iterable[IzLoggingFilter], underlying: LogSink): IzFilteringSink = {
      new IzFilteringSink(filters, underlying)
    }
  }
}
