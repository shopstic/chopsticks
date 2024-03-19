package dev.chopsticks.metric

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{Scope, URIO}

trait MetricRegistryFactory[Grp <: MetricGroup] {
  def manage: URIO[Scope, MetricRegistry[Grp]]
}
