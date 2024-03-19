package dev.chopsticks.dstream.metric

import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import zio.RLayer

trait DstreamStateMetrics {
  def workerCount: MetricGauge
  def offersTotal: MetricCounter
  def queueSize: MetricGauge
  def mapSize: MetricGauge
}

object DstreamStateMetrics {
  object Labels {
    final object serviceId extends MetricLabel
  }

  import Labels._

  sealed trait DstreamStateMetric extends MetricGroup

  final case object dstreamStateWorkerCount extends GaugeConfig(LabelNames of serviceId) with DstreamStateMetric
  final case object dstreamStateOffersTotal extends CounterConfig(LabelNames of serviceId) with DstreamStateMetric
  final case object dstreamStateQueueSize extends GaugeConfig(LabelNames of serviceId) with DstreamStateMetric
  final case object dstreamStateMapSize extends GaugeConfig(LabelNames of serviceId) with DstreamStateMetric
}

object DstreamStateMetricsManager {
  import DstreamStateMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamStateMetric], DstreamStateMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry[DstreamStateMetric], config: String) => {
      val labels = LabelValues.of(Labels.serviceId -> config)

      new DstreamStateMetrics {
        override val workerCount: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateWorkerCount, labels)
        override val offersTotal: MetricCounter =
          registry.counterWithLabels(DstreamStateMetrics.dstreamStateOffersTotal, labels)
        override val queueSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateQueueSize, labels)
        override val mapSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateMapSize, labels)
      }
    })
  }
}
