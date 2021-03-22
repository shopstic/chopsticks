package dev.chopsticks.sample.dstream

import com.typesafe.config.{Config, ConfigValueFactory}
import dev.chopsticks.dstream.metric.DstreamStateMetrics.DstreamStateMetric
import dev.chopsticks.dstream.DstreamState
import dev.chopsticks.dstream.metric.DstreamStateMetricsManager
import dev.chopsticks.fp.DiLayers
import dev.chopsticks.metric.prom.PromMetricRegistryFactory
import dev.chopsticks.sample.app.dstream.proto.load_test.{Assignment, Result}
import dev.chopsticks.sample.app.dstream.{AdditionConfig, DstreamLoadTestMasterAppConfig}
import dev.chopsticks.testkit.AkkaDiRunnableSpec
import io.prometheus.client.CollectorRegistry
import izumi.distage.model.definition
import zio.{Task, ZLayer}

import scala.concurrent.duration._

abstract class DstreamDiRunnableSpec extends AkkaDiRunnableSpec {

  protected val masterConfig: DstreamLoadTestMasterAppConfig = DstreamLoadTestMasterAppConfig(
    port = 0,
    partitions = 5,
    addition = AdditionConfig(
      from = 1,
      to = 100,
      iterations = 10
    ),
    expected = BigInt("295000"),
    distributionRetryInterval = 5.millis,
    idleTimeout = 30.seconds
  )

  override protected val loadConfig: Config = {
    super.loadConfig.withValue("iz-logging.level", ConfigValueFactory.fromAnyRef("Crit"))
  }

  override protected def testEnv(config: Config): Task[definition.Module] = Task {
    DiLayers(
      ZLayer.succeed(masterConfig),
      ZLayer.succeed(CollectorRegistry.defaultRegistry),
      PromMetricRegistryFactory.live[DstreamStateMetric]("test"),
      DstreamStateMetricsManager.live,
      DstreamState.manage[Assignment, Result]("test")
    )
  }

}
