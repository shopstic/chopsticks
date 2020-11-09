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
  def workerGauge: MetricGauge
  def attemptCounter: MetricCounter
  def queueGauge: MetricGauge
  def mapGauge: MetricGauge
}

object DstreamStateMetrics {
  final object dstreamStateLabel extends MetricLabel
  sealed trait DstreamStateMetricsGroup extends MetricGroup
  final case object workerGauge extends GaugeConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
  final case object attemptCounter extends CounterConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
  final case object queueGauge extends GaugeConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
  final case object mapGauge extends GaugeConfig(LabelNames of dstreamStateLabel) with DstreamStateMetricsGroup
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
            override val workerGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.workerGauge, labels)
            override val attemptCounter: MetricCounter =
              registry.counterWithLabels(DstreamStateMetrics.attemptCounter, labels)
            override val queueGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.queueGauge, labels)
            override val mapGauge: MetricGauge =
              registry.gaugeWithLabels(DstreamStateMetrics.mapGauge, labels)
          }
        }
      }
    MetricServiceManager.live(metricsServiceFactory)
  }
}
