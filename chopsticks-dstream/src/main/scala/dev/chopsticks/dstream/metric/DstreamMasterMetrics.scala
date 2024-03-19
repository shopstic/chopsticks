package dev.chopsticks.dstream.metric

import dev.chopsticks.metric.MetricConfigs.{CounterConfig, LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import dev.chopsticks.metric.{MetricCounter, MetricRegistry, MetricRegistryFactory, MetricServiceManager}
import zio.RLayer

trait DstreamMasterMetrics {
  def assignmentsTotal: MetricCounter
  def attemptsTotal: MetricCounter
  def successesTotal: MetricCounter
  def failuresTotal: MetricCounter
}

object DstreamMasterMetrics {
  object Labels {
    final object serviceId extends MetricLabel
    final object outcome extends MetricLabel
  }

  import Labels._

  sealed trait DstreamMasterMetric extends MetricGroup

  final case object dstreamMasterAssignmentsTotal
      extends CounterConfig(LabelNames of serviceId)
      with DstreamMasterMetric

  final case object dstreamMasterAttemptsTotal extends CounterConfig(LabelNames of serviceId) with DstreamMasterMetric

  final case object dstreamMasterResultsTotal
      extends CounterConfig(LabelNames of serviceId and outcome)
      with DstreamMasterMetric
}

object DstreamMasterMetricsManager {
  import DstreamMasterMetrics._

  def live: RLayer[MetricRegistryFactory[DstreamMasterMetric], DstreamMasterMetricsManager] = {
    MetricServiceManager.live((registry: MetricRegistry[DstreamMasterMetric], serviceId: String) => {
      new DstreamMasterMetrics {
        override val assignmentsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamMasterMetrics.dstreamMasterAssignmentsTotal,
            LabelValues
              .of(Labels.serviceId -> serviceId)
          )

        override val attemptsTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamMasterMetrics.dstreamMasterAttemptsTotal,
            LabelValues
              .of(Labels.serviceId -> serviceId)
          )

        override val successesTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamMasterMetrics.dstreamMasterResultsTotal,
            LabelValues
              .of(Labels.serviceId -> serviceId)
              .and(Labels.outcome -> "success")
          )

        override val failuresTotal: MetricCounter =
          registry.counterWithLabels(
            DstreamMasterMetrics.dstreamMasterResultsTotal,
            LabelValues
              .of(Labels.serviceId -> serviceId)
              .and(Labels.outcome -> "failure")
          )
      }
    })
  }
}
