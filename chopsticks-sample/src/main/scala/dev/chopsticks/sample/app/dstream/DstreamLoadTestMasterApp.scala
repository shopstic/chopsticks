/*
package dev.chopsticks.sample.app.dstream

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.Dstreams.DstreamServerConfig
import dev.chopsticks.dstream._
import dev.chopsticks.dstream.metric.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.metric.DstreamStateMetricsManager
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.load_test._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import io.prometheus.client.CollectorRegistry
import pureconfig.ConfigConvert
import zio._
import zio.clock.Clock

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

object DstreamLoadTestMasterApp extends ZAkkaApp {
  val currentValue = new AtomicReference(BigInt(0))
  private val counter = new LongAdder()

  private lazy val serviceId = "dstream_load_test_master"

  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    import zio.magic._

    app
      .injectSome[ZAkkaAppEnv](
        TypedConfig.live[DstreamLoadTestMasterAppConfig](),
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetric](serviceId),
        DstreamStateMetricsManager.live,
        DstreamState.manage[Assignment, Result](serviceId).toLayer
      )
      .as(ExitCode(0))
  }

  //noinspection TypeAnnotation
  def app = {
    manageServer.use { _ =>
      calculateResult.unit
    }
  }

  private[sample] def manageServer = {
    for {
      appConfig <- TypedConfig.get[DstreamLoadTestMasterAppConfig].toManaged_
      akkaRuntime <- ZManaged.runtime[AkkaEnv with IzLogging with Clock]
      dstreamState <- ZManaged.access[DstreamState[Assignment, Result]](_.get)
      binding <- Dstreams
        .manageServer(DstreamServerConfig(port = appConfig.port, idleTimeout = appConfig.idleTimeout)) {
          UIO {
            implicit val rt: Runtime[AkkaEnv with IzLogging with Clock] = akkaRuntime
            implicit val as: ActorSystem = akkaRuntime.environment.get.actorSystem

            StreamMasterPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)
            }
          }
        }
    } yield binding
  }

  private[sample] def calculateResult = {
    for {
      appConfig <- TypedConfig.get[DstreamLoadTestMasterAppConfig]
      logger <- ZIO.access[IzLogging](_.get.logger)
      result <- runMaster.log("Master")
      _ <- Task {
        val matched = if (result == appConfig.expected) "Yes" else "No"
        logger.info("STREAM COMPLETED **************************************")
        logger.info(s"Result: $result. Matched? $matched")
      }
    } yield result
  }

  private[sample] def runMaster = {
    for {
      appConfig <- TypedConfig.get[DstreamLoadTestMasterAppConfig]
      ks = KillSwitches.shared("server shared killswitch")
      result <- Source(1 to appConfig.partitions)
        .map(v =>
          Assignment(
            addition = v,
            from = appConfig.addition.from,
            to = appConfig.addition.to,
            iteration = appConfig.addition.iterations
          )
        )
        .toZAkkaSource
        .mapAsyncUnordered(12) { assignment =>
          Dstreams
            .distribute(ZIO.succeed(assignment)) { result: WorkResult[Result] =>
              val workerId = result.metadata.getText(Dstreams.WORKER_ID_HEADER).get

              result
                .source
                .map { result =>
                  result.body.errorCode
                    .map(errorCode => throw new RuntimeException(s"Obtained error result $errorCode"))
                    .getOrElse(result.getValue)
                }
                .viaMat(ks.flow)(Keep.right)
                .toZAkkaSource
                .interruptibleRunWith(Sink.fold(BigInt(0)) { case (s, r) =>
                  counter.increment()
                  s + r
                })
                .log(s"$assignment $workerId", logTraceOnError = false)
            }
            .retry(Schedule.fixed(appConfig.distributionRetryInterval.toJava))
        }
        .viaMat(ks.flow)(Keep.right)
        .interruptibleRunWith(Sink.fold(BigInt(0)) { (s, v) =>
          val newValue = s + v
          currentValue.set(newValue)
          newValue
        })
    } yield result
  }

}
 */
