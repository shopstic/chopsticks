package dev.chopsticks.fp.iz_logging

import org.apache.pekko.stream.RestartSettings
import com.typesafe.config.Config
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.util.config.PureconfigLoader
import izumi.fundamentals.platform.time.IzTimeSafe
import izumi.logstage.api.Log
import izumi.logstage.api.Log.CustomContext
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.api.rendering.logunits.Styler.TrimType
import izumi.logstage.api.rendering.logunits.{Extractor, Renderer, Styler}
import izumi.logstage.api.rendering.{RenderingOptions, StringRenderingPolicy}
import izumi.logstage.sink.file.models.{FileRotation, FileSinkConfig}
import izumi.logstage.sink.file.{FileServiceImpl, FileSink}
import logstage._
import zio.{RIO, RLayer, URIO, ZIO, ZLayer}

import scala.concurrent.duration.DurationInt

trait IzLogging {
  def logger: IzLogger
  def loggerWithCtx(ctx: LogCtx): IzLogger
  def zioLogger: LogIO3[ZIO]
  def zioLoggerWithCtx(ctx: LogCtx): LogIO3[ZIO]
  def logLevel: Log.Level
}

object IzLogging {
  final case class LiveService(logger: IzLogger, zioLogger: LogIO3[ZIO], logLevel: Log.Level) extends IzLogging {
    override def loggerWithCtx(ctx: LogCtx): IzLogger =
      logger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
        .withCustomContext(CustomContext(ctx.logArgs))

    override def zioLoggerWithCtx(ctx: LogCtx): LogIO3[ZIO] =
      zioLogger(IzLoggingCustomRenderers.LocationCtxKey -> ctx.sourceLocation)
  }

  def logger: URIO[IzLogging, IzLogger] = ZIO.serviceWith[IzLogging](_.logger)
  def loggerWithContext(ctx: LogCtx): URIO[IzLogging, IzLogger] = ZIO.serviceWith[IzLogging](_.loggerWithCtx(ctx))
  def zioLoggerWithContext(ctx: LogCtx): URIO[IzLogging, LogIO3[ZIO]] =
    ZIO.serviceWith[IzLogging](_.zioLoggerWithCtx(ctx))
  def zioLogger: URIO[IzLogging, LogIO3[ZIO]] = ZIO.serviceWith[IzLogging](_.zioLogger)

  def unsafeCreate(
    config: IzLoggingConfig,
    routerFactory: IzLoggingRouter,
    pekkoSvc: PekkoEnv
  ): LiveService = {
    val useLogBuffer = config.sinks.values
      .iterator
      .collect { case sinkConfig if sinkConfig.enabled => sinkConfig }
      .collectFirst { sinkConfig =>
        sinkConfig.destination match {
          case _: IzLoggingFileDestination => true
          case _ => false
        }
      }
      .getOrElse(false)

    val sinks = config.sinks.values.collect {
      case sinkConfig if sinkConfig.enabled =>
        val renderingPolicy = sinkConfig.format match {
          case IzLoggingTextFormat(withExceptions, withoutColors) =>
            new StringRenderingPolicy(
              RenderingOptions(withExceptions = withExceptions, colored = !withoutColors),
              template = Some(IzLogTemplates.consoleLayout)
            )

          case IzLoggingJsonFormat(prettyPrint) =>
            LogstageCirceRenderingPolicy(prettyPrint = prettyPrint)
        }

        sinkConfig.destination match {
          case IzLoggingConsoleDestination() => ConsoleSink(renderingPolicy)
          case IzLoggingFileDestination(path, rotationMaxFileCount, rotationMaxFileBytes) =>
            object jsonFileSink
                extends FileSink(
                  renderingPolicy = renderingPolicy,
                  fileService = new FileServiceImpl(path.value),
                  rotation = FileRotation.FileLimiterRotation(rotationMaxFileCount.value),
                  config = FileSinkConfig.inBytes(rotationMaxFileBytes.value)
                ) {
              override def recoverOnFail(e: String): Unit = Console.err.println(e)
            }
            jsonFileSink

          case IzLoggingTcpDestination(host, port, bufferSize) =>
            val tcpFlowRestartSettings = {
              import scala.concurrent.duration._
              RestartSettings(minBackoff = 100.millis, maxBackoff = 5.seconds, randomFactor = 0.2)
            }

            new IzLoggingTcpSink(
              host = host,
              port = port,
              renderingPolicy = renderingPolicy,
              bufferSize = bufferSize,
              tcpFlowRestartSettings = tcpFlowRestartSettings,
              pekkoSvc = pekkoSvc
            )
        }
    }.toList

    val buffer =
      if (!useLogBuffer) LogQueue.Immediate
      else {
        val buffer = new ThreadingLogQueue(50.millis, 128)
        buffer.start()
        buffer
      }
    val logger =
      IzLogger(routerFactory.create(config.level, sinks, buffer))(IzLoggingCustomRenderers.LoggerTypeCtxKey -> "iz")
    val zioLogger = LogZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

    // configure slf4j to use LogStage router
    StaticLogRouter.instance.setup(logger.router)
    LiveService(logger, zioLogger, config.level)
  }

  def unsafeLoadConfig(hoconConfig: Config, configNamespace: String = "iz-logging"): IzLoggingConfig = {
    PureconfigLoader.unsafeLoad[IzLoggingConfig](hoconConfig, configNamespace)
  }

  def create(config: IzLoggingConfig): RIO[PekkoEnv with IzLoggingRouter, LiveService] = {
    for {
      izLoggingRouterSvc <- ZIO.service[IzLoggingRouter]
      akkaSvc <- ZIO.service[PekkoEnv]
      svc <- ZIO.attempt {
        unsafeCreate(config, izLoggingRouterSvc, akkaSvc)
      }
    } yield svc
  }

  def live(config: IzLoggingConfig): RLayer[PekkoEnv with IzLoggingRouter, IzLogging] = {
    val managed = ZIO.acquireRelease {
      create(config)
    } { service =>
      ZIO.succeed(service.logger.router.close())
    }

    ZLayer.scoped(managed)
  }

  def live(hoconConfig: Config): RLayer[PekkoEnv with IzLoggingRouter, IzLogging] = {
    val effect = for {
      config <- ZIO.attempt(unsafeLoadConfig(hoconConfig))
      service <- create(config)
    } yield service

    ZLayer(effect)
  }

  def live(): RLayer[PekkoEnv with IzLoggingRouter with HoconConfig, IzLogging] = {
    val managed = for {
      hoconConfig <- HoconConfig.get
      service <- live(hoconConfig).build.map(_.get)
    } yield service

    ZLayer.scoped(managed)
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
