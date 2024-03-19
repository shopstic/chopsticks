package dev.chopsticks.sample.app.dstream

import org.apache.pekko.actor.ActorSystem
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
import dev.chopsticks.fp.config.TypedConfig
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.load_test._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
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

object DstreamLoadTestMasterApp extends ZPekkoApp {
  val currentValue = new AtomicReference(BigInt(0))
  private val counter = new LongAdder()

  private lazy val serviceId = "dstream_load_test_master"

  override def run: RIO[ZAkkaAppEnv with Scope, Any] = {
    app
      .provideSome[ZAkkaAppEnv with Scope](
        TypedConfig.live[DstreamLoadTestMasterAppConfig](),
        ZLayer.succeed(CollectorRegistry.defaultRegistry),
        PromMetricRegistryFactory.live[DstreamStateMetric](serviceId),
        DstreamStateMetricsManager.live,
        ZLayer.scoped(DstreamState.manage[Assignment, Result](serviceId))
      )
  }

  //noinspection TypeAnnotation
  def app = {
    manageServer.zipRight {
      calculateResult.unit
    }
  }

  private[sample] def manageServer = {
    for {
      appConfig <- TypedConfig.get[DstreamLoadTestMasterAppConfig]
      pekkoSvc <- ZIO.service[PekkoEnv]
      rt <- ZIO.runtime[PekkoEnv with IzLogging]
      dstreamState <- ZIO.service[DstreamState[Assignment, Result]]
      binding <- Dstreams
        .manageServer(DstreamServerConfig(port = appConfig.port, idleTimeout = appConfig.idleTimeout)) {
          ZIO.succeed {
            implicit val as: ActorSystem = pekkoSvc.actorSystem

            StreamMasterPowerApiHandler {
              (source, metadata) =>
                Dstreams.handle[Assignment, Result](dstreamState, source, metadata)(rt)
            }
          }
        }
    } yield binding
  }

  private[sample] def calculateResult = {
    for {
      appConfig <- TypedConfig.get[DstreamLoadTestMasterAppConfig]
      logger <- ZIO.serviceWith[IzLogging](_.logger)
      result <- runMaster.log("Master")
      _ <- ZIO.attempt {
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
