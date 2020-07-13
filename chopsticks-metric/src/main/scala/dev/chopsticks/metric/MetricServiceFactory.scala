package dev.chopsticks.metric

import dev.chopsticks.metric.MetricRegistry.MetricGroup

trait MetricServiceFactory[Grp <: MetricGroup, Cfg, Svc] {
  def create(registry: MetricRegistry.Service[Grp], config: Cfg): Svc
}
