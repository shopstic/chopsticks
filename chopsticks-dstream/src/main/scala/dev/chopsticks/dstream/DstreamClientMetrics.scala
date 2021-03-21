package dev.chopsticks.dstream

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, GaugeConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.{MetricCounter, MetricGauge, MetricRegistry, MetricRegistryFactory, MetricServiceManager}
import zio.RLayer

trait DstreamClientMetrics {
  def workerStatus: MetricGauge
  def attemptsTotal: MetricCounter
  def successesTotal: MetricCounter
  def timeoutsTotal: MetricCounter
  def failuresTotal: MetricCounter
}

object DstreamClientMetrics {
  object Labels {
    final object workerId extends MetricLabel
    final object outcome extends MetricLabel
  }

  import Labels._

  sealed trait DstreamClientMetric extends MetricGroup

  final case object dstreamWorkerStatus extends GaugeConfig(LabelNames of workerId) with DstreamClientMetric
  final case object dstreamWorkerAttemptsTotal extends CounterConfig(LabelNames of workerId) with DstreamClientMetric
  final case object dstreamWorkerResultsTotal
      extends CounterConfig(LabelNames of workerId and outcome)
      with DstreamClientMetric
}

object DstreamClientMetricsManager {
  import DstreamClientMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamClientMetric], DstreamClientMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry.Service[DstreamClientMetric], workerId: String) => {
      new DstreamClientMetrics {
        override val workerStatus: MetricGauge =
          registry.gaugeWithLabels(
            DstreamClientMetrics.dstreamWorkerStatus,
            LabelValues.of(Labels.workerId -> workerId)
          )

        override val attemptsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamClientMetrics.dstreamWorkerAttemptsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
          )

        override val successesTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamClientMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "success")
          )

        override val timeoutsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamClientMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "timeout")
          )

        override val failuresTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamClientMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "failure")
          )
      }
    })
  }
}
