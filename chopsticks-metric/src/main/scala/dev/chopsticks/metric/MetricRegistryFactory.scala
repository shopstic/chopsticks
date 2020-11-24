package dev.chopsticks.metric

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.UManaged

object MetricRegistryFactory {
  trait Service[Grp <: MetricGroup] {
    def manage: UManaged[MetricRegistry.Service[Grp]]
  }
}
