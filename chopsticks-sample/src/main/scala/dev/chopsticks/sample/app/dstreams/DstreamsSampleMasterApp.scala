package dev.chopsticks.sample.app.dstreams

import java.util.concurrent.atomic.{AtomicReference, LongAdder}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.config.Config
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetricsGroup
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.{MeasuredLogging, _}
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.dstreams.proto.grpc_akka_master_worker._
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import pureconfig.ConfigConvert
import zio._

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

final case class AdditionConfig(from: Int, to: Int, iterations: Int)

final case class DstreamsSampleMasterAppConfig(
  port: Int,
  partitions: Int,
  addition: AdditionConfig,
  expected: BigInt,
  distributionRetryInterval: FiniteDuration
)

object DstreamsSampleMasterAppConfig {
  //noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[DstreamsSampleMasterAppConfig]
  }
}

object DstreamsSampleMasterApp extends AkkaDiApp[DstreamsSampleMasterAppConfig] {
  val currentValue = new AtomicReference(BigInt(0))
  private val counter = new LongAdder()

  private lazy val serviceId = "dstreams_sample_master_app"

  override def config(allConfig: Config): Task[DstreamsSampleMasterAppConfig] = {
    Task(PureconfigLoader.unsafeLoad[DstreamsSampleMasterAppConfig](allConfig, "app"))
  }

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: DstreamsSampleMasterAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        ZLayer.succeed(appConfig),
        ZLayer.succeed(PromMetricRegistry.live[DstreamStateMetricsGroup](serviceId)),
        DstreamStateMetricsManager.live,
        DstreamStateFactory.live,
        AppLayer(app)
      )
      LiveDiEnv(extraLayers ++ akkaAppDi)
    }
  }

  def app: RIO[
    AkkaEnv with MeasuredLogging with DstreamStateFactory with DstreamsSampleMasterApp.AppConfig,
    Unit
  ] = {
    manageServer.use { case (_, dstreamState) =>
      calculateResult(dstreamState).unit
    }
  }

  def manageServer: RManaged[
    AkkaEnv with MeasuredLogging with DstreamStateFactory with AppConfig,
    (Http.ServerBinding, DstreamState.Service[Assignment, Result])
  ] = {
    for {
      appConfig <- ZManaged.service[DstreamsSampleMasterAppConfig]
      dstreamStateFactory <- ZManaged.access[DstreamStateFactory](_.get)
      dstreamState <- dstreamStateFactory.createStateService[Assignment, Result](serviceId)
      akkaService <- ZManaged.access[AkkaEnv](_.get)
      binding <- Dstreams.createManagedServer(
        DstreamServerConfig(port = appConfig.port, idleTimeout = 30.seconds), {
          ZIO.runtime[AkkaEnv].map { implicit rt =>
            import akkaService.actorSystem
            StreamMasterPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
            }
          }
        }
      )
    } yield (binding, dstreamState)
  }

  def calculateResult(dstreamState: DstreamState.Service[Assignment, Result])
    : RIO[AkkaEnv with MeasuredLogging with DstreamStateFactory with AppConfig, BigInt] = {
    for {
      appConfig <- ZIO.service[DstreamsSampleMasterAppConfig]
      logger <- ZIO.access[IzLogging](_.get.logger)
      result <- runMaster(dstreamState).log("Running master")
      _ <- Task {
        val matched = if (result == appConfig.expected) "Yes" else "No"
        logger.info("STREAM COMPLETED **************************************")
        logger.info(s"Result: $result. Matched? $matched")
      }
    } yield result
  }

  private def runMaster(dstreamState: DstreamState.Service[Assignment, Result]) = {
    for {
      appConfig <- ZIO.service[DstreamsSampleMasterAppConfig]
      logger <- ZIO.access[IzLogging](_.get.logger)
      graph <- ZIO.runtime[AkkaEnv with MeasuredLogging].map { implicit rt =>
        val ks = KillSwitches.shared("server shared killswitch")
        implicit val as: ActorSystem = rt.environment.get[AkkaEnv.Service].actorSystem
        import as.dispatcher

        Source(1 to appConfig.partitions)
          .map(v =>
            Assignment(
              addition = v,
              from = appConfig.addition.from,
              to = appConfig.addition.to,
              iteration = appConfig.addition.iterations
            )
          )
          .via(ZAkkaStreams.interruptibleMapAsyncUnordered(12) { assignment: Assignment =>
            Dstreams
              .distribute(dstreamState)(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
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
              .log(s"Distribute $assignment", logTraceOnError = false)
              .retry(Schedule.fixed(appConfig.distributionRetryInterval.toJava))
          })
          .viaMat(ks.flow)(Keep.right)
          .toMat(Sink.fold(BigInt(0)) { (s, v) =>
            val newValue = s + v
            currentValue.set(newValue)
            newValue
          })(Keep.both)
      }

      result <- ZAkkaStreams.interruptibleGraph(graph, graceful = true)
    } yield result
  }

}
