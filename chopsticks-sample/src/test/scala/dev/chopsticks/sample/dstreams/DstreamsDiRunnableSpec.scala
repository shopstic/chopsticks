package dev.chopsticks.sample.dstreams

import com.typesafe.config.{Config, ConfigValueFactory}
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetricsGroup
import dev.chopsticks.dstream.{DstreamStateFactory, DstreamStateMetricsManager}
import dev.chopsticks.fp.DiLayers
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.dstreams.{AdditionConfig, DstreamsSampleMasterAppConfig}
import dev.chopsticks.testkit.AkkaDiRunnableSpec
import izumi.distage.model.definition
import zio.{Task, ZLayer}

import scala.concurrent.duration._

abstract class DstreamsDiRunnableSpec extends AkkaDiRunnableSpec {

  protected val masterConfig: DstreamsSampleMasterAppConfig = DstreamsSampleMasterAppConfig(
    port = 0,
    partitions = 5,
    addition = AdditionConfig(
      from = 1,
      to = 100,
      iterations = 10
    ),
    expected = BigInt("295000"),
    distributionRetryInterval = 5.millis
  )

  override protected val loadConfig: Config = {
    super.loadConfig.withValue("iz-logging.level", ConfigValueFactory.fromAnyRef("Crit"))
  }

  override protected def testEnv(config: Config): Task[definition.Module] = Task {
    DiLayers(
      ZLayer.succeed(PromMetricRegistry.live[DstreamStateMetricsGroup]("MasterWorkerTest")),
      DstreamStateMetricsManager.live,
      ZLayer.succeed(masterConfig),
      DstreamStateFactory.live
    )
  }

}
