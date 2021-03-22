package dev.chopsticks.dstream

import dev.chopsticks.metric.MetricServiceManager

package object metric {
  type DstreamStateMetricsManager = MetricServiceManager[String, DstreamStateMetrics]
  type DstreamMasterMetricsManager = MetricServiceManager[String, DstreamMasterMetrics]
  type DstreamWorkerMetricsManager = MetricServiceManager[String, DstreamWorkerMetrics]
}
