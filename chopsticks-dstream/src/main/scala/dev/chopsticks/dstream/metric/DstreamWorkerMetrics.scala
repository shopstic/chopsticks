package dev.chopsticks.dstream.metric

import dev.chopsticks.metric.MetricConfigs._
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric._
import zio.RLayer

trait DstreamWorkerMetrics {
  def workerStatus: MetricGauge
  def attemptsTotal: MetricCounter
  def successesTotal: MetricCounter
  def timeoutsTotal: MetricCounter
  def failuresTotal: MetricCounter
}

object DstreamWorkerMetrics {
  object Labels {
    final object workerId extends MetricLabel
    final object outcome extends MetricLabel
  }

  import Labels._

  sealed trait DstreamWorkerMetric extends MetricGroup

  final case object dstreamWorkerStatus extends GaugeConfig(LabelNames of workerId) with DstreamWorkerMetric
  final case object dstreamWorkerAttemptsTotal extends CounterConfig(LabelNames of workerId) with DstreamWorkerMetric
  final case object dstreamWorkerResultsTotal
      extends CounterConfig(LabelNames of workerId and outcome)
      with DstreamWorkerMetric
}

object DstreamClientMetricsManager {
  import DstreamWorkerMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamWorkerMetric], DstreamWorkerMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry.Service[DstreamWorkerMetric], workerId: String) => {
      new DstreamWorkerMetrics {
        override val workerStatus: MetricGauge =
          registry.gaugeWithLabels(
            DstreamWorkerMetrics.dstreamWorkerStatus,
            LabelValues.of(Labels.workerId -> workerId)
          )

        override val attemptsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamWorkerMetrics.dstreamWorkerAttemptsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
          )

        override val successesTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamWorkerMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "success")
          )

        override val timeoutsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamWorkerMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "timeout")
          )

        override val failuresTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamWorkerMetrics.dstreamWorkerResultsTotal,
            LabelValues
              .of(Labels.workerId -> workerId)
              .and(Labels.outcome -> "failure")
          )
      }
    })
  }
}
