package dev.chopsticks.fp.iz_logging

import akka.stream.RestartSettings
import com.typesafe.config.Config
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.util.config.PureconfigLoader
import izumi.fundamentals.platform.time.IzTimeSafe
import izumi.logstage.api.Log
import izumi.logstage.api.Log.CustomContext
import izumi.logstage.api.rendering.json.LogstageCirceRenderingPolicy
import izumi.logstage.api.rendering.logunits.Styler.TrimType
import izumi.logstage.api.rendering.logunits.{Extractor, Renderer, Styler}
import izumi.logstage.api.rendering.{RenderingOptions, StringRenderingPolicy}
import izumi.logstage.sink.QueueingSink
import izumi.logstage.sink.file.models.{FileRotation, FileSinkConfig}
import izumi.logstage.sink.file.{FileServiceImpl, FileSink}
import logstage._
import zio._

object IzLogging {
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
  def zioLoggerWithContext(ctx: LogCtx): URIO[IzLogging, LogIO3[ZIO]] =
    ZIO.access[IzLogging](_.get.zioLoggerWithCtx(ctx))
  def zioLogger: URIO[IzLogging, LogIO3[ZIO]] = ZIO.access[IzLogging](_.get.zioLogger)

  def unsafeCreate(
    config: IzLoggingConfig,
    routerFactory: IzLoggingRouter.Service,
    akkaSvc: AkkaEnv.Service
  ): LiveService = {
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

            val queueingSink = new QueueingSink(jsonFileSink)
            queueingSink.start()
            queueingSink

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
              akkaSvc = akkaSvc
            )
        }
    }.toList

    val logger =
      IzLogger(routerFactory.create(config.level, sinks))(IzLoggingCustomRenderers.LoggerTypeCtxKey -> "iz")
    val zioLogger = LogZIO.withDynamicContext(logger)(ZIO.succeed(CustomContext.empty))

    // configure slf4j to use LogStage router
    StaticLogRouter.instance.setup(logger.router)
    LiveService(logger, zioLogger, config.level)
  }

  def unsafeLoadConfig(hoconConfig: Config, configNamespace: String = "iz-logging"): IzLoggingConfig = {
    PureconfigLoader.unsafeLoad[IzLoggingConfig](hoconConfig, configNamespace)
  }

  def create(config: IzLoggingConfig): RIO[AkkaEnv with IzLoggingRouter, LiveService] = {
    for {
      izLoggingRouterSvc <- ZIO.access[IzLoggingRouter](_.get)
      akkaSvc <- ZIO.access[AkkaEnv](_.get)
      svc <- Task {
        unsafeCreate(config, izLoggingRouterSvc, akkaSvc)
      }
    } yield svc
  }

  def live(config: IzLoggingConfig): RLayer[AkkaEnv with IzLoggingRouter, IzLogging] = {
    val managed = ZManaged.make {
      create(config)
    } { service =>
      UIO(service.logger.router.close())
    }

    managed.toLayer
  }

  def live(hoconConfig: Config): RLayer[AkkaEnv with IzLoggingRouter, IzLogging] = {
    val effect = for {
      config <- Task(unsafeLoadConfig(hoconConfig))
      service <- create(config)
    } yield service

    effect.toLayer
  }

  def live(): RLayer[AkkaEnv with IzLoggingRouter with HoconConfig, IzLogging] = {
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
