package dev.chopsticks.sample.app.dstream

import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.dstream.metric.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.metric.DstreamStateMetricsManager
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.util.LoggedRace
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto._
import dev.chopsticks.stream.ZAkkaFlow
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import io.grpc.StatusRuntimeException
import io.prometheus.client.CollectorRegistry
import zio.{RIO, Schedule, Scope, ZIO, ZLayer}

import scala.collection.immutable.ListMap
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamSampleApp extends ZPekkoApp {
  private lazy val serviceId = "dstream_sample_app"

  override def run: RIO[ZAkkaAppEnv with Scope, Any] = {
    app
      .provideSome[PekkoEnv with IzLogging with Scope](
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetric](serviceId),
        DstreamStateMetricsManager.live,
        ZLayer.scoped(DstreamState.manage[Assignment, Result](serviceId))
      )
  }

  //noinspection TypeAnnotation
  def app = {
    LoggedRace()
      .add("Run", start)
      .add("Periodic logging", periodicallyLog)
      .run()
  }

  private def periodicallyLog = {
    val io = for {
      zlogger <- ZIO.serviceWith[IzLogging](_.zioLogger)
      dstreamMetrics <- ZIO.serviceWithZIO[DstreamStateMetricsManager](_.activeSet)
      metricsMap <- ZIO.succeed {
        ListMap(
          "workers" -> dstreamMetrics.iterator.map(_.workerCount.get).sum.toString,
          "attempts" -> dstreamMetrics.iterator.map(_.offersTotal.get).sum.toString,
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
      ks <- ZIO.succeed(KillSwitches.shared("server shared killswitch"))
      logger <- ZIO.serviceWith[IzLogging](_.logger)
      workDistributionFlow = ZAkkaFlow[Assignment]
        .mapAsyncUnordered(12) { assignment =>
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
                .retry(Schedule.forever.tapInput((e: Throwable) => ZIO.succeed(logger.error(s"Distribute failed: $e"))))
            }
        }
      result <- Source(1 to 100)
        .map(Assignment(_))
        .toZAkkaSource
        .viaZAkkaFlow(workDistributionFlow)
        .viaMat(ks.flow)(Keep.right)
        .interruptibleRunIgnore()
    } yield result
  }

  protected def runWorker(
    client: DstreamSampleServiceClient,
    id: Int
  ): RIO[PekkoEnv with MeasuredLogging, Unit] = {
    Dstreams
      .work(client.work().addHeader(Dstreams.WORKER_ID_HEADER, id.toString)) { assignment =>
        ZIO.succeed {
          Source
            .single(1)
            .map(v => Result(assignment.valueIn * 10 + v))
        }
      }
  }

  private def start = {
    for {
      pekkoSvc <- ZIO.service[PekkoEnv]
      rt <- ZIO.runtime[PekkoEnv]
      dstreamState <- ZIO.service[DstreamState[Assignment, Result]]
      _ <- Dstreams
        .manageServer(DstreamServerConfig(port = 9999, idleTimeout = 30.seconds)) {
          ZIO.succeed {
            import pekkoSvc.actorSystem

            DstreamSampleServicePowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)(rt)
            }
          }
        }
      client <- Dstreams
        .manageClient {
          ZIO.succeed {
            import pekkoSvc.actorSystem

            DstreamSampleServiceClient(
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
      masterFiber <- runMaster
        .log("Master")
        .fork
      workersFiber <- ZIO.forkAll {
        (1 to 8).map { id =>
          runWorker(client, id)
            .foldZIO(
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
}
