package dev.chopsticks

import dev.chopsticks.fp.util.SharedResourceManager
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{Has, ULayer}

package object metric {
  type MetricRegistryFactory[C <: MetricGroup] = Has[ULayer[MetricRegistry[C]]]
  type MetricRegistry[C <: MetricGroup] = Has[MetricRegistry.Service[C]]
  type MetricServiceManager[Cfg, Svc] = SharedResourceManager[Any, Cfg, Svc]
}
