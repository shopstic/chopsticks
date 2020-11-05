package dev.chopsticks.sample.app

import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, _}
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetrics
import dev.chopsticks.metric.{MetricCounter, MetricGauge}
import dev.chopsticks.sample.app.proto.grpc_akka_master_worker._
import dev.chopsticks.stream.ZAkkaStreams
import io.prometheus.client.{Counter, Gauge}
import zio._

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object GrpcAkkaMasterApp extends AkkaDiApp[Unit] {
  lazy val port = sys.env.getOrElse("SERVER_PORT", "9999").toInt
  lazy val concurrency =
    sys.env.get("CONCURRENCY").map(_.toInt).getOrElse(java.lang.Runtime.getRuntime.availableProcessors())
  lazy val partitions = sys.env.get("PARTITIONS").map(_.toInt).getOrElse(100)
  lazy val iteration = sys.env.get("ITERATION").map(_.toInt).getOrElse(100)
  lazy val expected = sys.env.get("EXPECTED").map(BigInt.apply).getOrElse(BigInt("160128167199229050000"))

  private val counter = new LongAdder()

  private object metrics extends DstreamStateMetrics {
    val workerGauge: MetricGauge =
      new PromMetrics.PromGauge(Gauge.build("dstream_workers", "dstream_workers").register())
    val attemptCounter: MetricCounter =
      new PromMetrics.PromCounter(Counter.build("dstream_attempts_total", "dstream_attempts_total").register())
    val queueGauge: MetricGauge =
      new PromMetrics.PromGauge(Gauge.build("dstream_queue", "dstream_queue").register())
    val mapGauge: MetricGauge =
      new PromMetrics.PromGauge(Gauge.build("dstream_map", "dstream_map").register())
  }

  override def config(allConfig: Config): Task[Unit] = Task.unit

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: Unit,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        DstreamStateFactory.live,
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  private def app = {
    val managedZio = for {
      dstreamStateFactory <- ZManaged.access[DstreamStateFactory](_.get)
      dstreamState <- dstreamStateFactory.createStateService[Assignment, Result](metrics)
      akkaService <- ZManaged.access[AkkaEnv](_.get)
      _ <- Dstreams.createManagedServer(
        DstreamServerConfig(port = port, idleTimeout = 30.seconds), {
          ZIO.runtime[AkkaEnv].map { implicit rt =>
            import akkaService.actorSystem
            StreamMasterPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
            }
          }
        }
      )
    } yield {
      for {
        logger <- ZIO.access[IzLogging](_.get.logger)
        result <- runMaster(dstreamState).log("Running master")
        _ <- Task {
          val matched = if (result == expected) "Yes" else "No"
          logger.info("STREAM COMPLETED **************************************")
          logger.info(s"Result: $result. Matched? $matched")
        }
      } yield ()
    }
    managedZio.use(identity)
  }

  private def runMaster(dstreamState: DstreamState.Service[Assignment, Result]) = {
    for {
      logger <- ZIO.access[IzLogging](_.get.logger)
      graph <- ZIO.runtime[AkkaEnv with MeasuredLogging].map { implicit rt =>
        val ks = KillSwitches.shared("server shared killswitch")
        implicit val as: ActorSystem = rt.environment.get[AkkaEnv.Service].actorSystem
        import as.dispatcher

        Source(1 to partitions)
          .map(v => Assignment(addition = v, from = 1, to = 178956970, iteration = iteration))
          .via(ZAkkaStreams.interruptibleMapAsyncUnordered(12) { assignment: Assignment =>
            Dstreams
              .distribute(dstreamState, assignment) { result: WorkResult[Result] =>
                val workerId = result.metadata.getText(Dstreams.WORKER_ID_HEADER).get
                UIO(logger.info(s"$assignment assigned to $workerId")) *>
                  Task
                    .fromFuture { _ =>
                      result
                        .source
                        .map { result =>
                          result.body.errorCode
                            .map(errorCode => throw new RuntimeException(s"Obtained error result $errorCode"))
                            .getOrElse(result.getValue)
                        }
                        .via(ks.flow)
                        .runFold(BigInt(0)) { case (s, r) =>
                          counter.increment()
                          s + r
                        }
                        .map { r =>
                          logger.info(s"Source drained $assignment")
                          r
                        }
                    }
              }
              .log(s"Distribute $assignment")
              .retry(Schedule.fixed(250.millis.toJava).tapInput((e: Throwable) =>
                ZIO.accessM[IzLogging](
                  _.get.zioLogger.error(s"Distribute failed for $assignment, it will be retried. Cause: $e")
                )
              ))
          })
          .viaMat(ks.flow)(Keep.right)
          .toMat(Sink.fold(BigInt(0))((s, v) => s + v))(Keep.both)
      }

      result <- ZAkkaStreams.interruptibleGraph(graph, graceful = true)
    } yield result
  }

}
