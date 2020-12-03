package dev.chopsticks.dstream

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, GaugeConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.{MetricCounter, MetricGauge, MetricRegistry, MetricRegistryFactory, MetricServiceManager}
import zio.RLayer

trait DstreamStateMetrics {
  def dstreamWorkers: MetricGauge
  def dstreamAttemptsTotal: MetricCounter
  def dstreamQueueSize: MetricGauge
  def dstreamMapSize: MetricGauge
}

object DstreamStateMetrics {
  object Labels {
    final object id extends MetricLabel
  }

  import Labels._

  sealed trait DstreamStateMetric extends MetricGroup

  final case object dstreamWorkers extends GaugeConfig(LabelNames of id) with DstreamStateMetric
  final case object dstreamAttemptsTotal extends CounterConfig(LabelNames of id) with DstreamStateMetric
  final case object dstreamQueueSize extends GaugeConfig(LabelNames of id) with DstreamStateMetric
  final case object dstreamMapSize extends GaugeConfig(LabelNames of id) with DstreamStateMetric
}

object DstreamStateMetricsManager {
  import DstreamStateMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamStateMetric], DstreamStateMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry.Service[DstreamStateMetric], config: String) => {
      val labels = LabelValues.of(Labels.id -> config)

      new DstreamStateMetrics {
        override val dstreamWorkers: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamWorkers, labels)
        override val dstreamAttemptsTotal: MetricCounter =
          registry.counterWithLabels(DstreamStateMetrics.dstreamAttemptsTotal, labels)
        override val dstreamQueueSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamQueueSize, labels)
        override val dstreamMapSize: MetricGauge =
          registry.gaugeWithLabels(DstreamStateMetrics.dstreamMapSize, labels)
      }
    })
  }
}
