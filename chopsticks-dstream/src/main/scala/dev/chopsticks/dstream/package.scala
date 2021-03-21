package dev.chopsticks

import dev.chopsticks.metric.MetricServiceManager
import zio._

package object dstream {
  type DstreamState[Assignment, Result] = Has[DstreamState.Service[Assignment, Result]]
  type DstreamWorker[Assignment, Result] = Has[DstreamWorker.Service[Assignment, Result]]
  type DstreamMaster[In, Assignment, Result, Out] = Has[DstreamMaster.Service[In, Assignment, Result, Out]]
  type DstreamServer[Assignment, Result] = Has[DstreamServer.Service[Assignment, Result]]
  type DstreamServerHandlerFactory[Assignment, Result] = Has[DstreamServerHandlerFactory.Service[Assignment, Result]]
  type DstreamServerHandler[Assignment, Result] = Has[DstreamServerHandler.Service[Assignment, Result]]
  type DstreamClient[Assignment, Result] = Has[DstreamClient.Service[Assignment, Result]]
  type DstreamStateFactory = Has[DstreamStateFactory.Service]
  type DstreamStateMetricsManager = MetricServiceManager[String, DstreamStateMetrics]
  type DstreamClientMetricsManager = MetricServiceManager[String, DstreamClientMetrics]
}
