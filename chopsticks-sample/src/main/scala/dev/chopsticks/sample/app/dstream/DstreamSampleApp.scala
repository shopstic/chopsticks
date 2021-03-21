package dev.chopsticks.sample.app.dstream

import akka.grpc.GrpcClientSettings
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
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
import dev.chopsticks.stream.ZAkkaFlow
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
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
        DstreamState.manage[Assignment, Result](serviceId),
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  //noinspection TypeAnnotation
  def app = {
    for {
      runFib <- run.fork
      periodicLoggingFib <- periodicallyLog.fork
      _ <- TaskUtils.raceFirst(
        List(
          "Run" -> runFib.join,
          "Periodic logging" -> periodicLoggingFib.join
        )
      )
    } yield ()
  }

  private def periodicallyLog = {
    val io = for {
      zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
      dstreamMetrics <- ZIO.accessM[DstreamStateMetricsManager](_.get.activeSet)
      metricsMap <- UIO {
        ListMap(
          "workers" -> dstreamMetrics.iterator.map(_.workerCount.get).sum.toString,
          "attempts" -> dstreamMetrics.iterator.map(_.attemptsTotal.get).sum.toString,
          "queue" -> dstreamMetrics.iterator.map(_.queueSize.get).sum.toString,
          "map" -> dstreamMetrics.iterator.map(_.mapSize.get).sum.toString
        )
      }
      formatted = metricsMap.iterator.map { case (k, v) => s"$k=$v" }.mkString(" ")
      _ <- zlogger.info(s"${formatted -> "formatted" -> null}")
    } yield ()
    io.repeat(Schedule.fixed(1.second.toJava)).unit
  }

  private def runMaster = {
    for {
      ks <- UIO(KillSwitches.shared("server shared killswitch"))
      logger <- ZIO.access[IzLogging](_.get.logger)
      workDistributionFlow <- ZAkkaFlow[Assignment]
        .interruptibleMapAsyncUnordered(12) { assignment =>
          Dstreams
            .distribute(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
              result
                .source
                .viaMat(ks.flow)(Keep.right)
                .take(1)
                .toZAkkaSource
                .interruptibleRunWith(Sink.foreach { item =>
                  logger.info(
                    s"Server < ${result.metadata.getText(Dstreams.WORKER_ID_HEADER) -> "worker"} ${assignment.valueIn -> "assignment"} $item"
                  )
                })
                .retry(Schedule.forever.tapInput((e: Throwable) => UIO(logger.error(s"Distribute failed: $e"))))
            }
        }
        .make
      result <- Source(1 to 100)
        .map(Assignment(_))
        .via(workDistributionFlow)
        .viaMat(ks.flow)(Keep.right)
        .toZAkkaSource
        .interruptibleRunIgnore()
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

  private def run = {
    val managed = for {
      akkaRuntime <- ZManaged.runtime[AkkaEnv with MeasuredLogging]
      akkaSvc = akkaRuntime.environment.get
      dstreamState <- ZManaged.access[DstreamState[Assignment, Result]](_.get)
      _ <- Dstreams
        .manageServer(DstreamServerConfig(port = 9999, idleTimeout = 30.seconds)) {
          UIO {
            implicit val rt: Runtime[AkkaEnv with MeasuredLogging] = akkaRuntime
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
        masterFiber <- runMaster
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
