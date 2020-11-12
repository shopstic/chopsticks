package dev.chopsticks.fp.iz_logging

import com.typesafe.config.{Config => LbConfig}
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
import zio.{Layer, UIO, ZIO, ZManaged}

object IzLogging {

  final case class JsonFileSinkConfig(
    path: String,
    rotationMaxFileCount: Int,
    rotationMaxFileBytes: Int
  )

  final case class IzLoggingConfig(level: Level, coloredOutput: Boolean, jsonFileSink: Option[JsonFileSinkConfig])

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
    def zioLogger: LogBIO3[ZIO]
    def zioLoggerWithCtx(ctx: LogCtx): LogBIO3[ZIO]
  }

  final case class LiveService(logger: IzLogger, zioLogger: LogBIO3[ZIO]) extends Service {
    override def loggerWithCtx(ctx: LogCtx): IzLogger =
      logger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
    override def zioLoggerWithCtx(ctx: LogCtx): LogBIO3[ZIO] =
      zioLogger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
  }

  def create(lbConfig: LbConfig): Service = {
    create(lbConfig, List.empty)
  }

  def create(lbConfig: LbConfig, filters: Iterable[IzLoggingFilter]): Service = {
    create(lbConfig, "iz-logging", filters)
  }

  def create(lbConfig: LbConfig, namespace: String, filters: Iterable[IzLoggingFilter]): Service = {
    create(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace), filters)
  }

  def create(config: IzLoggingConfig): Service = {
    create(config, List.empty)
  }

  def create(config: IzLoggingConfig, filters: Iterable[IzLoggingFilter]): Service = {
    val consoleSink = {
      val renderingPolicy =
        if (config.coloredOutput) RenderingPolicy.coloringPolicy(Some(IzLogTemplates.consoleLayout))
        else RenderingPolicy.simplePolicy(Some(IzLogTemplates.consoleLayout))
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

    val sinks = (consoleSink :: maybeFileSink.toList).map(sink => IzLoggingSinks.IzFilteringSink(filters, sink))
    val logger = IzLogger(config.level, sinks)(IzLoggingCustomRenderers.LoggerTypeCtxKey -> "iz")
    val zioLogger = LogstageZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

    maybeFileSink.foreach(_.start())

    // configure slf4j to use LogStage router
    StaticLogRouter.instance.setup(logger.router)
    LiveService(logger, zioLogger)
  }

  def live(lbConfig: LbConfig): Layer[Nothing, IzLogging] = {
    live(lbConfig, List.empty)
  }

  def live(lbConfig: LbConfig, filters: Iterable[IzLoggingFilter]): Layer[Nothing, IzLogging] = {
    live(lbConfig, "iz-logging", filters)
  }

  def live(lbConfig: LbConfig, namespace: String, filters: Iterable[IzLoggingFilter]): Layer[Nothing, IzLogging] = {
    live(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace), filters)
  }

  def live(config: IzLoggingConfig): Layer[Nothing, IzLogging] = {
    live(config, List.empty)
  }

  def live(config: IzLoggingConfig, filters: Iterable[IzLoggingFilter]): Layer[Nothing, IzLogging] = {
    val managed: ZManaged[Any, Nothing, Service] = for {
      service <- ZManaged.make {
        UIO(create(config, filters))
      } { service =>
        UIO(service.logger.router.close())
      }
    } yield service
    managed.toLayer
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
