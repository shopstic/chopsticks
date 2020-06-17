package dev.chopsticks

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{Has, ULayer}

package object metric {
  type MetricServiceManager[C <: MetricGroup, Svc] = Has[MetricServiceManager.Service[C, Svc]]
  type MetricRegistryFactory[C <: MetricGroup] = Has[ULayer[MetricRegistry[C]]]
  type MetricRegistry[C <: MetricGroup] = Has[MetricRegistry.Service[C]]
}
