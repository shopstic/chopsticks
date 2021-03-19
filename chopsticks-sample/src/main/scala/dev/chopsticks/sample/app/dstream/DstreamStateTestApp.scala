package dev.chopsticks.sample.app.dstream

import akka.stream.scaladsl.{Sink, Source}
import dev.chopsticks.dstream.DstreamClientApi.DstreamClientApiConfig
import dev.chopsticks.dstream.DstreamClientRunner.DstreamClientConfig
import dev.chopsticks.dstream.DstreamServer.DstreamServerConfig
import dev.chopsticks.dstream.DstreamServerRunner.DstreamServerRunnerConfig
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream._
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.fp.zio_ext.ZManagedExtensions
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.{
  Assignment,
  DstreamSampleAppClient,
  DstreamSampleAppPowerApiHandler,
  Result
}
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.prometheus.client.CollectorRegistry
import zio.{ExitCode, RIO, Schedule, UIO, ZIO, ZLayer, ZManaged}

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamStateTestApp extends ZAkkaApp {

  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    import zio.magic._

    val promRegistry = ZLayer.succeed(CollectorRegistry.defaultRegistry)
    val promRegistryFactory = PromMetricRegistryFactory.live[DstreamStateMetric]("test")
    val dstreamStateMetricsManager = DstreamStateMetricsManager.live
    val dstreamState = DstreamState.manage[Assignment, Result]("test").toLayer
    val dstreamServerHandlerFactory = DstreamServerHandlerFactory.live[Assignment, Result] { handle =>
      ZIO
        .access[AkkaEnv](_.get.actorSystem)
        .map { implicit as =>
          DstreamSampleAppPowerApiHandler(handle(_, _))
        }
    }
    val dstreamServerHandler = DstreamServerHandler.live[Assignment, Result]
    val dstreamClientApi = DstreamClientApi
      .live[Assignment, Result](DstreamClientApiConfig(
        serverHost = "localhost",
        serverPort = 9999,
        withTls = false
      )) { settings =>
        ZIO
          .access[AkkaEnv](_.get.actorSystem)
          .map { implicit as =>
            DstreamSampleAppClient(settings)
          }
      }(_.work())

    val dstreamServer = DstreamServer.live[Assignment, Result]
    val dstreamServerRunner = DstreamServerRunner.live[Assignment, Assignment, Result, Result]
    val dstreamClientRunner = DstreamClientRunner.live[Assignment, Result]()

    app
      .as(ExitCode(1))
      .provideSomeMagicLayer[ZAkkaAppEnv](
        promRegistry,
        promRegistryFactory,
        dstreamStateMetricsManager,
        dstreamState,
        dstreamServerHandlerFactory,
        dstreamServerHandler,
        dstreamClientApi,
        dstreamServer,
        dstreamServerRunner,
        dstreamClientRunner
      )
  }

  //noinspection TypeAnnotation
  def app = {
    LoggedRace()
      .add("master", runMaster)
      .add("worker", runWorker)
      .add("logging", periodicallyLog)
      .run()
  }

  private lazy val parallelism: PosInt = 4

  private def runMaster = {
    val managed = for {
      server <- ZManaged.access[DstreamServer[Assignment, Result]](_.get)
      _ <- server.manage(DstreamServerConfig(port = 9999))
        .logResult("Dstream server", _.localAddress.toString)
    } yield ()

    managed.use { _ =>
      for {
        runner <- ZIO
          .access[DstreamServerRunner[Assignment, Assignment, Result, Result]](_.get)
        distributionFlow <- runner
          .createFlow(
            DstreamServerRunnerConfig(parallelism = parallelism, ordered = false),
            ZIO.succeed(_)
          ) {
            (assignment, result) =>
              for {
                zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
                last <- result
                  .source
                  .toZAkkaSource
                  .interruptibleRunWith(Sink.last)
                _ <- zlogger.debug(
                  s"Server < ${result.metadata.getText(Dstreams.WORKER_ID_HEADER) -> "worker"} ${assignment.valueIn -> "assignment"} $last"
                )
              } yield last
          }
        _ <- Source(1 to Int.MaxValue)
          //        .initialDelay(1.minute)
          .map(Assignment(_))
          .via(distributionFlow)
          .toZAkkaSource
          .interruptibleRunIgnore()
      } yield ()
    }
  }

  private def runWorker = {
    for {
      dstreamClient <- ZIO.access[DstreamClientRunner[Assignment, Result]](_.get)
      _ <- dstreamClient
        .run(DstreamClientConfig(
          parallelism = parallelism,
          assignmentTimeout = 10.seconds,
          retryInitialDelay = 100.millis,
          retryBackoffFactor = 2.0,
          retryMaxDelay = 1.second,
          retryResetAfter = 5.seconds
        )) { assignment =>
          UIO {
            Source(1 to 10)
              .map(v => Result(assignment.valueIn * 10 + v))
//              .throttle(1, 1.second)
          }
        }
    } yield ()
  }

  private def periodicallyLog = {
    val task = for {
      zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
      dstreamMetrics <- ZIO.accessM[DstreamStateMetricsManager](_.get.activeSet)
      _ <- ZIO.effectSuspend {
        zlogger
          .withCustomContext(
            "workers" -> dstreamMetrics.iterator.map(_.dstreamWorkers.get).sum.toString,
            "attempts" -> dstreamMetrics.iterator.map(_.dstreamAttemptsTotal.get).sum.toString,
            "queue" -> dstreamMetrics.iterator.map(_.dstreamQueueSize.get).sum.toString,
            "map" -> dstreamMetrics.iterator.map(_.dstreamMapSize.get).sum.toString
          )
          .info("")
      }
    } yield ()

    task.repeat(Schedule.fixed(1.second.toJava)).unit
  }
}
