package dev.chopsticks

import dev.chopsticks.metric.MetricServiceManager
import zio._

package object dstream {
  type DstreamState[Assignment, Result] = Has[DstreamState.Service[Assignment, Result]]
  type DstreamClientRunner[Assignment, Result] = Has[DstreamClientRunner.Service[Assignment, Result]]
  type DstreamServerRunner[In, Assignment, Result, Out] = Has[DstreamServerRunner.Service[In, Assignment, Result, Out]]
  type DstreamServer[Assignment, Result] = Has[DstreamServer.Service[Assignment, Result]]
  type DstreamServerHandlerFactory[Assignment, Result] = Has[DstreamServerHandlerFactory.Service[Assignment, Result]]
  type DstreamServerHandler[Assignment, Result] = Has[DstreamServerHandler.Service[Assignment, Result]]
  type DstreamClientApi[Assignment, Result] = Has[DstreamClientApi.Service[Assignment, Result]]
  type DstreamStateFactory = Has[DstreamStateFactory.Service]
  type DstreamStateMetricsManager = MetricServiceManager[String, DstreamStateMetrics]
}
