package dev.chopsticks.sample.app.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream.{DstreamState, DstreamStateMetricsManager, Dstreams}
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.TaskUtils
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.{
  Assignment,
  DstreamSampleAppClient,
  DstreamSampleAppPowerApiHandler,
  Result
}
import dev.chopsticks.stream.{ZAkkaFlow, ZAkkaStreams}
import io.prometheus.client.CollectorRegistry
import zio.{Runtime, Schedule, Task, UIO, ZIO, ZLayer, ZManaged}

import java.util.concurrent.TimeoutException
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamStateTestApp extends AkkaDiApp[NotUsed] {
  override def config(allConfig: Config): Task[NotUsed] = Task.unit.as(NotUsed)

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: NotUsed,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(akkaAppDi ++ DiLayers(
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetric]("test"),
        DstreamStateMetricsManager.live,
        DstreamState.manage[Assignment, Result]("test"),
        AppLayer(app)
      ))
    }
  }

  private def runMaster = {
    for {
      zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
      workDistributionFlow <- ZAkkaFlow[Assignment].interruptibleMapAsyncUnordered(1) { assignment =>
        Dstreams
          .distribute(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
            ZAkkaStreams
              .interruptibleGraph(
                result
                  .source
                  .viaMat(KillSwitches.single)(Keep.right)
                  .toMat(Sink.last)(Keep.both),
                graceful = true
              )
              .flatMap { last =>
                zlogger.info(
                  s"Server < ${result.metadata.getText(Dstreams.WORKER_ID_HEADER) -> "worker"} ${assignment.valueIn -> "assignment"} $last"
                )
              }
          }
      }
      _ <- ZAkkaStreams
        .interruptibleGraph(
          Source(1 to 100)
            .initialDelay(10.seconds)
            .map(Assignment(_))
            .via(workDistributionFlow)
            .viaMat(KillSwitches.single)(Keep.right)
            .toMat(Sink.ignore)(Keep.both),
          graceful = true
        )
    } yield ()
  }

  private def runWorker(client: DstreamSampleAppClient) = {
    for {
      zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
      _ <- zlogger.info("Worker start")
      resultPromise <- UIO(Promise[Source[Result, NotUsed]]())
      flow <- ZAkkaFlow[Assignment].interruptibleMapAsync(1) { assignment =>
        UIO {
          Source(1 to 10)
            .map(v => Result(assignment.valueIn * 10 + v))
            .throttle(1, 1.second)
        }
          .tap(s => UIO(resultPromise.success(s)))
          .zipRight(Task.fromFuture(_ => resultPromise.future))
      }
      result <- ZAkkaStreams
        .interruptibleGraph(
          client
            .work(Source.futureSource(resultPromise.future).mapMaterializedValue(_ => NotUsed))
            .viaMat(KillSwitches.single)(Keep.right)
            .initialTimeout(1.second)
            .via(flow)
            .toMat(Sink.ignore)(Keep.both),
          graceful = true
        )
    } yield result
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

  //noinspection TypeAnnotation
  def app = {
    val managed = for {
      akkaRuntime <- ZManaged.runtime[AkkaEnv]
      akkaSvc = akkaRuntime.environment.get
      dstreamState <- ZManaged.access[DstreamState[Assignment, Result]](_.get)
      _ <- Dstreams
        .manageServer(DstreamServerConfig(port = 9999, idleTimeout = 30.seconds)) {
          UIO {
            implicit val rt: Runtime[AkkaEnv] = akkaRuntime
            import akkaSvc.actorSystem

            DstreamSampleAppPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
            }
          }
        }
      client <- Dstreams
        .manageClient {
          UIO {
            import akkaSvc.actorSystem

            DstreamSampleAppClient(
              GrpcClientSettings
                .connectToServiceAt("localhost", 9999)
                .withTls(false)
            )
          }
        }
    } yield {
      for {
        loggingFib <- periodicallyLog.fork
        masterFiber <- runMaster
          .fork
        workersFiber <- runWorker(client)
          .repeatWhile(_ => true)
          .retryWhile {
            case _: TimeoutException => true
            case _ => false
          }
          .unit
          .fork
        _ <- TaskUtils
          .raceFirst(Map(
            "Master" -> masterFiber.join,
            "Worker" -> workersFiber.join,
            "Logging" -> loggingFib.join
          ))
      } yield ()
    }

    managed.use(identity)
  }
}
