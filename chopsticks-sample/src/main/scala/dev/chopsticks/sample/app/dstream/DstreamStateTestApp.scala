package dev.chopsticks.sample.app.dstream

import akka.stream.scaladsl.{Sink, Source}
import dev.chopsticks.dstream.DstreamClientMetrics.DstreamClientMetric
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.DstreamServer.DstreamServerConfig
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.DstreamWorker.{AkkaGrpcBackend, DstreamWorkerConfig, DstreamWorkerRetryConfig}
import dev.chopsticks.dstream._
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.fp.zio_ext.ZManagedExtensions
import dev.chopsticks.metric.log.MetricLogger
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

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

object DstreamStateTestApp extends ZAkkaApp {

  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    import zio.magic._

    val promRegistry = ZLayer.succeed(CollectorRegistry.defaultRegistry)
    val stateMetricRegistryFactory = PromMetricRegistryFactory.live[DstreamStateMetric]("test")
    val clientMetricRegistryFactory = PromMetricRegistryFactory.live[DstreamClientMetric]("test")
    val dstreamStateMetricsManager = DstreamStateMetricsManager.live
    val dstreamClientMetricsManager = DstreamClientMetricsManager.live
    val dstreamState = DstreamState.manage[Assignment, Result]("test").toLayer
    val dstreamServerHandlerFactory = DstreamServerHandlerFactory.live[Assignment, Result] { handle =>
      ZIO
        .access[AkkaEnv](_.get.actorSystem)
        .map { implicit as =>
          DstreamSampleAppPowerApiHandler(handle(_, _))
        }
    }
    val dstreamServerHandler = DstreamServerHandler.live[Assignment, Result]
    val dstreamClient = DstreamClient
      .live[Assignment, Result] { settings =>
        ZIO
          .access[AkkaEnv](_.get.actorSystem)
          .map { implicit as =>
            DstreamSampleAppClient(settings)
          }
      } { (client, workerId) =>
        client.work().addHeader("dstream-worker-id", workerId.toString)
      }

    val dstreamServer = DstreamServer.live[Assignment, Result]
    val dstreamMaster = DstreamMaster.live[Assignment, Assignment, Result, Result]
    val dstreamWorker = DstreamWorker.live[Assignment, Result]
    val metricLogger = MetricLogger.live()

    app
      .as(ExitCode(1))
      .provideSomeMagicLayer[ZAkkaAppEnv](
        promRegistry,
        stateMetricRegistryFactory,
        clientMetricRegistryFactory,
        dstreamStateMetricsManager,
        dstreamClientMetricsManager,
        dstreamState,
        dstreamServerHandlerFactory,
        dstreamServerHandler,
        dstreamClient,
        dstreamServer,
        dstreamMaster,
        dstreamWorker,
        metricLogger
      )
  }

  //noinspection TypeAnnotation
  def app = {
    LoggedRace()
      .add("master", runMaster)
      .add("worker", runWorker)
      .add("metrics", logMetrics)
      .run()
  }

  private lazy val parallelism: PosInt = 1

  private def runMaster = {
    val managed = for {
      server <- ZManaged.access[DstreamServer[Assignment, Result]](_.get)
      _ <- server.manage(DstreamServerConfig(port = 9999))
        .logResult("Dstream server", _.localAddress.toString)
    } yield ()

    managed.use { _ =>
      for {
        master <- ZIO
          .access[DstreamMaster[Assignment, Assignment, Result, Result]](_.get)
        distributionFlow <- master
          .createFlow(
            DstreamMasterConfig(parallelism = parallelism, ordered = false),
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
          } {
            Schedule.stop
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
      worker <- ZIO.access[DstreamWorker[Assignment, Result]](_.get)
      _ <- worker
        .run(DstreamWorkerConfig(
          serverHost = "localhost",
          serverPort = 9999,
          serverTls = false,
          backend = AkkaGrpcBackend.Netty,
          parallelism = parallelism,
          assignmentTimeout = 10.seconds
        )) { assignment =>
          UIO {
            Source(1 to 10)
              .map(v => Result(assignment.valueIn * 10 + v))
              .throttle(1, 1.second)
          }
        } {
          DstreamWorker
            .createRetrySchedule(
              _,
              DstreamWorkerRetryConfig(
                retryInitialDelay = 100.millis,
                retryBackoffFactor = 2.0,
                retryMaxDelay = 1.second,
                retryResetAfter = 5.seconds
              )
            )
        }
    } yield ()
  }

  private def logMetrics = {
    MetricLogger
      .periodicallyCollect {
        for {
          stateMetrics <- ZIO.accessM[DstreamStateMetricsManager](_.get.activeSet)
          clientMetrics <- ZIO.accessM[DstreamClientMetricsManager](_.get.activeSet)
        } yield {
          import MetricLogger.sum

          ListMap(
            "state-workers" -> sum(stateMetrics)(_.workerCount),
            "state-attempts" -> sum(stateMetrics)(_.attemptsTotal),
            "state-queue" -> sum(stateMetrics)(_.queueSize),
            "state-map" -> sum(stateMetrics)(_.mapSize),
            "client-workers" -> sum(clientMetrics)(_.workerStatus),
            "client-attempts" -> sum(clientMetrics)(_.attemptsTotal),
            "client-successes" -> sum(clientMetrics)(_.successesTotal),
            "client-failures" -> sum(clientMetrics)(_.failuresTotal)
          )
        }
      }
  }
}
