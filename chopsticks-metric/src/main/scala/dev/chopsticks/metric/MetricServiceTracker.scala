package dev.chopsticks.metric

trait MetricServiceTracker[Svc] {
  def activeSet: Set[Svc]
}
