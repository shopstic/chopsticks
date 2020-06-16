package dev.chopsticks

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{Has, ULayer}

package object metric {
  type MetricRegistryManager[Id, C <: MetricGroup] = Has[MetricRegistryManager.Service[Id, C]]
  type MetricRegistryFactory[C <: MetricGroup] = Has[ULayer[MetricRegistry[C]]]
  type MetricRegistry[C <: MetricGroup] = Has[MetricRegistry.Service[C]]
}
