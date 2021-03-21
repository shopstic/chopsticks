package dev.chopsticks.dstream

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, GaugeConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.{MetricCounter, MetricGauge, MetricRegistry, MetricRegistryFactory, MetricServiceManager}
import zio.RLayer

trait DstreamStateMetrics {
  def workerCount: MetricGauge
  def attemptsTotal: MetricCounter
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
  final case object dstreamStateAttemptsTotal extends CounterConfig(LabelNames of serviceId) with DstreamStateMetric
  final case object dstreamStateQueueSize extends GaugeConfig(LabelNames of serviceId) with DstreamStateMetric
  final case object dstreamStateMapSize extends GaugeConfig(LabelNames of serviceId) with DstreamStateMetric
}

object DstreamStateMetricsManager {
  import DstreamStateMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamStateMetric], DstreamStateMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry.Service[DstreamStateMetric], config: String) => {
      val labels = LabelValues.of(Labels.serviceId -> config)

      new DstreamStateMetrics {
        override val workerCount: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateWorkerCount, labels)
        override val attemptsTotal: MetricCounter =
          registry.counterWithLabels(DstreamStateMetrics.dstreamStateAttemptsTotal, labels)
        override val queueSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateQueueSize, labels)
        override val mapSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamStateMapSize, labels)
      }
    })
  }
}
