package dev.chopsticks.sample.app.dstream

import akka.Done
import akka.grpc.GrpcClientSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.TaskUtils
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto._
import dev.chopsticks.stream.ZAkkaStreams
import io.grpc.StatusRuntimeException
import io.prometheus.client.CollectorRegistry
import zio._

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamSampleApp extends AkkaDiApp[Unit] {

  private lazy val serviceId = "dstream_sample_app"

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Unit,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetric](serviceId),
        DstreamStateMetricsManager.live,
        DstreamStateFactory.live,
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  //noinspection TypeAnnotation
  def app = {
    val managed = for {
      dstreamStateFactory <- ZManaged.access[DstreamStateFactory](_.get)
      dstreamState <- dstreamStateFactory.manage[Assignment, Result](serviceId)
    } yield {
      for {
        runFib <- run(dstreamState).fork
        periodicLoggingFib <- periodicallyLog.fork
        _ <- TaskUtils.raceFirst(
          List(
            "Run" -> runFib.join,
            "Periodic logging" -> periodicLoggingFib.join
          )
        )
      } yield ()
    }

    managed.use(identity)
  }

  private def periodicallyLog = {
    val io = for {
      zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
      dstreamMetrics <- ZIO.accessM[DstreamStateMetricsManager](_.get.activeSet)
      metricsMap <- UIO {
        ListMap(
          "workers" -> dstreamMetrics.iterator.map(_.dstreamWorkers.get).sum.toString,
          "attempts" -> dstreamMetrics.iterator.map(_.dstreamAttemptsTotal.get).sum.toString,
          "queue" -> dstreamMetrics.iterator.map(_.dstreamQueueSize.get).sum.toString,
          "map" -> dstreamMetrics.iterator.map(_.dstreamMapSize.get).sum.toString
        )
      }
      formatted = metricsMap.iterator.map { case (k, v) => s"$k=$v" }.mkString(" ")
      _ <- zlogger.info(s"${formatted -> "formatted" -> null}")
    } yield ()
    io.repeat(Schedule.fixed(1.second.toJava)).unit
  }

  private def runMaster(dstreamState: DstreamState.Service[Assignment, Result])
    : RIO[AkkaEnv with MeasuredLogging, Done] = {
    for {
      ks <- UIO(KillSwitches.shared("server shared killswitch"))
      logger <- ZIO.access[IzLogging](_.get.logger)
      workDistributionFlow <- ZAkkaStreams.interruptibleMapAsyncUnorderedM(12) { assignment: Assignment =>
        Dstreams
          .distribute(dstreamState)(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
            ZAkkaStreams
              .interruptibleGraph(
                result
                  .source
                  .via(ks.flow)
                  .take(1)
                  .toMat(Sink.foreach { item =>
                    logger.info(
                      s"Server < ${result.metadata.getText(Dstreams.WORKER_ID_HEADER) -> "worker"} ${assignment.valueIn -> "assignment"} $item"
                    )
                  })((_, future) => ks -> future),
                graceful = true
              )
              .retry(Schedule.forever.tapInput((e: Throwable) => UIO(logger.error(s"Distribute failed: $e"))))
          }
      }
      result <- ZAkkaStreams
        .interruptibleGraph(
          Source(1 to 100)
            .map(Assignment(_))
            .via(workDistributionFlow)
            .via(ks.flow)
            .toMat(Sink.ignore)((_, future) => ks -> future),
          graceful = true
        )
    } yield result
  }

  protected def runWorker(client: DstreamSampleAppClient, id: Int) = {
    Dstreams
      .work(client.work().addHeader(Dstreams.WORKER_ID_HEADER, id.toString)) { assignment =>
        UIO {
          Source
            .single(1)
            .map(v => Result(assignment.valueIn * 10 + v))
        }
      }
  }

  private def run(dstreamState: DstreamState.Service[Assignment, Result]) = {
    val managed = for {
      akkaRuntime <- ZManaged.runtime[AkkaEnv]
      akkaSvc = akkaRuntime.environment.get
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
              //          .withChannelBuilderOverrides(
              //            _.eventLoopGroup(new io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup(4))
              //              .channelType(classOf[io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel])
              //              .executor(env.dispatcher)
              //          )
            )
          }
        }
    } yield {
      for {
        masterFiber <- runMaster(dstreamState)
          .log("Master")
          .fork
        workersFiber <- ZIO.forkAll {
          (1 to 8).map { id =>
            runWorker(client, id)
              .foldM(
                {
                  case e: StatusRuntimeException => ZIO.fail(e)
                  case e => ZIO.left(e)
                },
                r => ZIO.right(r)
              )
              .log(s"Worker $id")
              .repeat(Schedule.fixed(1.second.toJava))
          }
        }
        _ <- masterFiber.join.raceFirst(workersFiber.join)
      } yield ()
    }

    managed.use(identity)
  }
}
