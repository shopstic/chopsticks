package dev.chopsticks.sample.app.dstream

import akka.actor.ActorSystem
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
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.load_test._
import dev.chopsticks.stream.ZAkkaStreams
import dev.chopsticks.util.config.PureconfigLoader
import io.prometheus.client.CollectorRegistry
import pureconfig.ConfigConvert
import zio._

import java.util.concurrent.atomic.{AtomicReference, LongAdder}
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

final case class AdditionConfig(from: Int, to: Int, iterations: Int)

final case class DstreamLoadTestMasterAppConfig(
  port: Int,
  partitions: Int,
  addition: AdditionConfig,
  expected: BigInt,
  distributionRetryInterval: FiniteDuration,
  idleTimeout: FiniteDuration
)

object DstreamLoadTestMasterAppConfig {
  //noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[DstreamLoadTestMasterAppConfig]
  }
}

object DstreamLoadTestMasterApp extends AkkaDiApp[DstreamLoadTestMasterAppConfig] {
  val currentValue = new AtomicReference(BigInt(0))
  private val counter = new LongAdder()

  private lazy val serviceId = "dstream_load_test_master"

  override def config(allConfig: Config): Task[DstreamLoadTestMasterAppConfig] = {
    Task(PureconfigLoader.unsafeLoad[DstreamLoadTestMasterAppConfig](allConfig, "app"))
  }

  override def liveEnv(
    akkaAppDi: DiModule,
    appConfig: DstreamLoadTestMasterAppConfig,
    allConfig: Config
  ): Task[DiEnv[AppEnv]] = {
    Task {
      val extraLayers = DiLayers(
        ZLayer.succeed(appConfig),
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
    manageServer.use { case (_, dstreamState) =>
      calculateResult(dstreamState).unit
    }
  }

  private[sample] def manageServer = {
    for {
      appConfig <- ZManaged.access[AppConfig](_.get)
      dstreamStateFactory <- ZManaged.access[DstreamStateFactory](_.get)
      dstreamState <- dstreamStateFactory.manage[Assignment, Result](serviceId)
      akkaRuntime <- ZManaged.runtime[AkkaEnv]
      binding <- Dstreams
        .manageServer(DstreamServerConfig(port = appConfig.port, idleTimeout = appConfig.idleTimeout)) {
          UIO {
            implicit val rt: Runtime[AkkaEnv] = akkaRuntime
            implicit val as: ActorSystem = akkaRuntime.environment.get.actorSystem

            StreamMasterPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
            }
          }
        }
    } yield (binding, dstreamState)
  }

  private[sample] def calculateResult(dstreamState: DstreamState.Service[Assignment, Result]) = {
    for {
      appConfig <- ZIO.access[AppConfig](_.get)
      logger <- ZIO.access[IzLogging](_.get.logger)
      result <- runMaster(dstreamState).log("Master")
      _ <- Task {
        val matched = if (result == appConfig.expected) "Yes" else "No"
        logger.info("STREAM COMPLETED **************************************")
        logger.info(s"Result: $result. Matched? $matched")
      }
    } yield result
  }

  private[sample] def runMaster(dstreamState: DstreamState.Service[Assignment, Result]) = {
    for {
      appConfig <- ZIO.access[AppConfig](_.get)
      ks = KillSwitches.shared("server shared killswitch")
      workDistributionFlow <- ZAkkaStreams.interruptibleMapAsyncUnorderedM(12) { assignment: Assignment =>
        Dstreams
          .distribute(dstreamState)(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
            val workerId = result.metadata.getText(Dstreams.WORKER_ID_HEADER).get

            ZAkkaStreams
              .interruptibleGraph(
                result
                  .source
                  .map { result =>
                    result.body.errorCode
                      .map(errorCode => throw new RuntimeException(s"Obtained error result $errorCode"))
                      .getOrElse(result.getValue)
                  }
                  .viaMat(ks.flow)(Keep.right)
                  .toMat(Sink.fold(BigInt(0)) { case (s, r) =>
                    counter.increment()
                    s + r
                  })(Keep.both),
                graceful = true
              )
              .log(s"$assignment $workerId", logTraceOnError = false)
          }
          .retry(Schedule.fixed(appConfig.distributionRetryInterval.toJava))
      }
      result <- ZAkkaStreams
        .interruptibleGraph(
          Source(1 to appConfig.partitions)
            .map(v =>
              Assignment(
                addition = v,
                from = appConfig.addition.from,
                to = appConfig.addition.to,
                iteration = appConfig.addition.iterations
              )
            )
            .via(workDistributionFlow)
            .viaMat(ks.flow)(Keep.right)
            .toMat(Sink.fold(BigInt(0)) { (s, v) =>
              val newValue = s + v
              currentValue.set(newValue)
              newValue
            })(Keep.both),
          graceful = true
        )
    } yield result
  }

}
