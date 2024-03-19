package dev.chopsticks.sample.app.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.DstreamServer.DstreamServerConfig
import dev.chopsticks.dstream.DstreamServerHandlerFactory.DstreamServerPartialHandler
import dev.chopsticks.dstream.DstreamWorker.{DstreamWorkerConfig, DstreamWorkerRetryConfig}
import dev.chopsticks.dstream._
import dev.chopsticks.dstream.metric.DstreamMasterMetrics.DstreamMasterMetric
import dev.chopsticks.dstream.metric.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.metric.DstreamWorkerMetrics.DstreamWorkerMetric
import dev.chopsticks.dstream.metric.{
  DstreamClientMetricsManager,
  DstreamMasterMetricsManager,
  DstreamStateMetricsManager,
  DstreamWorkerMetricsManager
}
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.metric.log.MetricLogger
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.prometheus.client.CollectorRegistry
import zio.{RIO, Scope, ZIO, ZLayer}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

object DstreamStateTestApp extends ZPekkoApp {

  override def run: RIO[ZAkkaAppEnv with Scope, Any] = {

    val promRegistry = ZLayer.succeed(CollectorRegistry.defaultRegistry)

    val stateMetricRegistryFactory = PromMetricRegistryFactory.live[DstreamStateMetric]("test")
    val workerMetricRegistryFactory = PromMetricRegistryFactory.live[DstreamWorkerMetric]("test")
    val masterMetricRegistryFactory = PromMetricRegistryFactory.live[DstreamMasterMetric]("test")

    val dstreamStateMetricsManager = DstreamStateMetricsManager.live
    val dstreamClientMetricsManager = DstreamClientMetricsManager.live
    val dstreamMasterMetricsManager = DstreamMasterMetricsManager.live

    val dstreamState = ZLayer.scoped(DstreamState.manage[Assignment, Result]("test"))
    val dstreamServerHandlerFactory = DstreamServerHandlerFactory.live[Assignment, Result] { handle =>
      ZIO
        .serviceWith[PekkoEnv](_.actorSystem)
        .map { implicit as =>
          DstreamServerPartialHandler(
            DstreamSampleServicePowerApiHandler.partial(handle(_, _)),
            DstreamSampleService
          )
        }
    }
    val dstreamServerHandler = DstreamServerHandler.live[Assignment, Result]
    val dstreamClient = DstreamClient
      .live[Assignment, Result] { settings =>
        ZIO
          .serviceWith[PekkoEnv](_.actorSystem)
          .map { implicit as =>
            DstreamSampleServiceClient(settings)
          }
      } { (client, workerId) =>
        client.work().addHeader("dstream-worker-id", workerId.toString)
      }

    val dstreamServer = DstreamServer.live[Assignment, Result]
    val dstreamMaster = DstreamMaster.live[Assignment, Assignment, Result, Result]
    val dstreamWorker = DstreamWorker.live[Assignment, Result, NotUsed]
    val metricLogger = MetricLogger.live()

    app
      .provideSome[PekkoEnv with IzLogging with Scope](
        promRegistry,
        stateMetricRegistryFactory,
        workerMetricRegistryFactory,
        masterMetricRegistryFactory,
        dstreamStateMetricsManager,
        dstreamClientMetricsManager,
        dstreamMasterMetricsManager,
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

  private lazy val parallelism: PosInt = 4

  private def runMaster = {
    for {
      server <- ZIO.service[DstreamServer[Assignment, Result]]
      _ <- server.manage(DstreamServerConfig(port = 9999))
        .logResult("Dstream server", _.localAddress.toString)
      master <- ZIO
        .service[DstreamMaster[Assignment, Assignment, Result, Result]]
      distributionFlow <- master
        .manageFlow(
          DstreamMasterConfig(serviceId = "test", parallelism = parallelism, ordered = false),
          ZIO.succeed(_)
        ) {
          (assignment, result) =>
            for {
              zlogger <- IzLogging.zioLogger
              last <- result
                .source
                .toZAkkaSource
                .killSwitch
                .interruptibleRunWith(Sink.last)
              _ <- zlogger.debug(
                s"Server < ${result.metadata.getText(Dstreams.WORKER_ID_HEADER) -> "worker"} ${assignment.valueIn -> "assignment"} $last"
              )
            } yield last
        }((_, task) => task)
      _ <- Source(1 to Int.MaxValue)
        .initialDelay(1.minute)
        .map(Assignment(_))
        .toZAkkaSource
        .viaZAkkaFlow(distributionFlow)
        .killSwitch
        .interruptibleRunIgnore()
    } yield ()
  }

  private def runWorker = {
    for {
      worker <- ZIO.service[DstreamWorker[Assignment, Result, NotUsed]]
      clientSettings <- PekkoEnv.actorSystem.map { implicit as =>
        GrpcClientSettings
          .connectToServiceAt("localhost", 9999)
          .withTls(false)
          .withBackend("netty")
      }
      _ <- worker
        .run(DstreamWorkerConfig(
          clientSettings = clientSettings,
          parallelism = parallelism,
          assignmentTimeout = 10.seconds
        )) { (_, assignment) =>
          ZIO.succeed {
            Source(1 to 10)
              .map(v => Result(assignment.valueIn * 10 + v))
//              .throttle(1, 1.second)
          }
        } { (workerId, task) =>
          task
            .forever
            .retry(DstreamWorker
              .createRetrySchedule(
                workerId,
                DstreamWorkerRetryConfig(
                  retryInitialDelay = 100.millis,
                  retryBackoffFactor = 2.0,
                  retryMaxDelay = 1.second,
                  retryResetAfter = 5.seconds
                )
              ))
            .as(NotUsed)

        }
    } yield ()
  }

  private def logMetrics = {
    MetricLogger
      .periodicallyCollect {
        for {
          stateMetrics <- ZIO.serviceWithZIO[DstreamStateMetricsManager](_.activeSet)
          masterMetrics <- ZIO.serviceWithZIO[DstreamMasterMetricsManager](_.activeSet)
          workerMetrics <- ZIO.serviceWithZIO[DstreamWorkerMetricsManager](_.activeSet)
        } yield {
          import MetricLogger.sum

          ListMap(
            // State
            "state-workers" -> sum(stateMetrics)(_.workerCount),
            "state-attempts" -> sum(stateMetrics)(_.offersTotal),
            "state-queue" -> sum(stateMetrics)(_.queueSize),
            "state-map" -> sum(stateMetrics)(_.mapSize),
            // Master
            "master-assignments" -> sum(masterMetrics)(_.assignmentsTotal),
            "master-attempts" -> sum(masterMetrics)(_.attemptsTotal),
            "master-successes" -> sum(masterMetrics)(_.successesTotal),
            "master-failures" -> sum(masterMetrics)(_.failuresTotal),
            // Worker
            "worker-workers" -> sum(workerMetrics)(_.workerStatus),
            "worker-attempts" -> sum(workerMetrics)(_.attemptsTotal),
            "worker-successes" -> sum(workerMetrics)(_.successesTotal),
            "worker-failures" -> sum(workerMetrics)(_.failuresTotal)
          )
        }
      }
  }
}
