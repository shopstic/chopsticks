package dev.chopsticks.dstream

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, GaugeConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.{
  MetricCounter,
  MetricGauge,
  MetricRegistry,
  MetricRegistryFactory,
  MetricServiceFactory,
  MetricServiceManager
}
import zio.RLayer

trait DstreamStateMetrics {
  def dstreamWorkerGauge: MetricGauge
  def dstreamAttemptCounter: MetricCounter
  def dstreamQueueGauge: MetricGauge
  def dstreamMapGauge: MetricGauge
}

object DstreamStateMetrics {
  final object dstreamStateLabel extends MetricLabel
  sealed trait DstreamStateMetricsGroup extends MetricGroup
  final case object dstreamWorkerGauge
      extends GaugeConfig(LabelNames of dstreamStateLabel)
      with DstreamStateMetricsGroup
  final case object dstreamAttemptCounter
      extends CounterConfig(LabelNames of dstreamStateLabel)
      with DstreamStateMetricsGroup
  final case object dstreamQueueGauge extends GaugeConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
  final case object dstreamMapGauge extends GaugeConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
}

object DstreamStateMetricsManager {
  import DstreamStateMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamStateMetricsGroup], DstreamStateMetricsManager] = {
    val metricsServiceFactory: MetricServiceFactory[DstreamStateMetricsGroup, String, DstreamStateMetrics] =
      new MetricServiceFactory[DstreamStateMetricsGroup, String, DstreamStateMetrics] {
        override def create(
          registry: MetricRegistry.Service[DstreamStateMetricsGroup],
          config: String
        ): DstreamStateMetrics = {
          val labels = LabelValues.of(dstreamStateLabel -> config)
          new DstreamStateMetrics {
            override val dstreamWorkerGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.dstreamWorkerGauge, labels)
            override val dstreamAttemptCounter: MetricCounter =
              registry.counterWithLabels(DstreamStateMetrics.dstreamAttemptCounter, labels)
            override val dstreamQueueGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.dstreamQueueGauge, labels)
            override val dstreamMapGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.dstreamMapGauge, labels)
          }
        }
      }
    MetricServiceManager.live(metricsServiceFactory)
  }
}
