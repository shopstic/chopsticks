package dev.chopsticks

import dev.chopsticks.metric.MetricServiceManager
import zio._

package object dstream {
  type DstreamState[Req, Res] = Has[DstreamState.Service[Req, Res]]
  type DstreamStateFactory = Has[DstreamStateFactory.Service]
  type DstreamStateMetricsManager = MetricServiceManager[String, DstreamStateMetrics]
}
