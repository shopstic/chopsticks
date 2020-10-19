package dev.chopsticks.fp.iz_logging

import com.typesafe.config.{Config => LbConfig}
import dev.chopsticks.util.config.PureconfigLoader
import izumi.fundamentals.platform.time.IzTimeSafe
import izumi.logstage.api.Log
import izumi.logstage.api.Log.{CustomContext, Level}
import izumi.logstage.api.rendering.{RenderingOptions, RenderingPolicy}
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.api.rendering.logunits.Styler.{PadType, TrimType}
import izumi.logstage.api.rendering.logunits.{Extractor, LETree, Renderer, Styler}
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
    def locationLogger(implicit line: sourcecode.Line, file: sourcecode.FileName): IzLogger
    def zioLogger: LogBIO3[ZIO]
    def zioLocationLogger(implicit line: sourcecode.Line, file: sourcecode.FileName): LogBIO3[ZIO]
  }

  final case class LiveService(logger: IzLogger, zioLogger: LogBIO3[ZIO]) extends Service {
    override def locationLogger(implicit line: sourcecode.Line, file: sourcecode.FileName) =
      logger(IzLogRenderingExtractors.LocationCtxKey -> s"${file.value}:${line.value}")
    override def zioLocationLogger(implicit line: sourcecode.Line, file: sourcecode.FileName) =
      zioLogger(IzLogRenderingExtractors.LocationCtxKey -> s"${file.value}:${line.value}")
  }

  def create(lbConfig: LbConfig, namespace: String = "iz-logging"): Service = {
    create(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace))
  }

  def create(config: IzLoggingConfig): Service = {
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

    val logger = IzLogger(config.level, sinks = consoleSink :: maybeFileSink.toList)(
      IzLogRenderingExtractors.LoggerTypeCtxKey -> "iz"
    )
    val zioLogger = LogstageZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

    maybeFileSink.foreach(_.start())

    // configure slf4j to use LogStage router
    StaticLogRouter.instance.setup(logger.router)
    LiveService(logger, zioLogger)
  }

  def live(lbConfig: LbConfig, namespace: String = "iz-logging"): Layer[Nothing, IzLogging] = {
    live(PureconfigLoader.unsafeLoad[IzLoggingConfig](lbConfig, namespace))
  }

  def live(config: IzLoggingConfig): Layer[Nothing, IzLogging] = {
    val managed: ZManaged[Any, Nothing, Service] = for {
      service <- ZManaged.make {
        UIO(create(config))
      } { service =>
        UIO(service.logger.router.close())
      }
    } yield service
    managed.toLayer
  }
}

object IzLogRenderingExtractors {
  val LocationCtxKey = "location"
  val LoggerTypeCtxKey = "loggerType"
  private val ExcludedCtxKeys = Set(LocationCtxKey, LoggerTypeCtxKey)

  class ContextSourcePositionExtractor(fallback: Extractor) extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions) = {
      entry.context.customContext.values.find(_.path.exists(_ == LocationCtxKey)) match {
        case Some(arg) =>
          val stringArg = if (arg.value == null) "null" else "(" + arg.value.toString + ")"
          LETree.TextNode(stringArg)
        case None =>
          fallback.render(entry, context)
      }
    }
  }

  class FilteringContextExtractor(fallback: Extractor) extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions) = {
      val originalCtx = entry.context.customContext
      val filteredCustomContext = {
        val filteredValues = originalCtx.values.filterNot(_.path.exists(ExcludedCtxKeys))
        originalCtx.copy(values = filteredValues)
      }
      val filteredEntry = entry.copy(context = entry.context.copy(customContext = filteredCustomContext))
      fallback.render(filteredEntry, context)
    }
  }

  class LocationExtractor(sourceExtractor: Extractor, loggerNameExtractor: Extractor) extends Extractor {
    override def render(entry: Log.Entry, context: RenderingOptions) = {
      entry.context.customContext.values.find(_.path.exists(_ == LoggerTypeCtxKey)) match {
        case Some(_) =>
          sourceExtractor.render(entry, context)
        case None =>
          loggerNameExtractor.render(entry, context)
      }
    }
  }

}

object IzLogTemplates {
  import IzLogRenderingExtractors._

  val consoleLayout = new Renderer.Aggregate(
    Seq(
      new Styler.LevelColor(
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
      new Extractor.Constant("["),
      new Styler.Trim(
        Seq(
          new LocationExtractor(
            sourceExtractor = new ContextSourcePositionExtractor(new Extractor.SourcePosition()),
            loggerNameExtractor = new Extractor.LoggerName()
          )
        ),
        42,
        TrimType.Left,
        Some("…")
      ),
      new Extractor.Constant("]"),
      Extractor.Space,
      new Extractor.Constant("["),
      new Styler.AdaptivePad(Seq(new Extractor.ThreadId()), 1, PadType.Left, ' '),
      new Extractor.Constant(":"),
      new Styler.AdaptivePad(
        Seq(new Styler.Trim(Seq(new Extractor.ThreadName()), 20, TrimType.Center, Some("…"))),
        4,
        PadType.Right,
        ' '
      ),
      new Extractor.Constant("]"),
      Extractor.Space,
      new Styler.TrailingSpace(Seq(new FilteringContextExtractor(new Extractor.LoggerContext()))),
      new Extractor.Message()
    )
  )
}
