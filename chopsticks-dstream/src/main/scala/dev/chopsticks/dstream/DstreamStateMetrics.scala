package dev.chopsticks.dstream

import dev.chopsticks.metric.{MetricCounter, MetricGauge}

trait DstreamStateMetrics {
  def workerGauge: MetricGauge
  def attemptCounter: MetricCounter
  def queueGauge: MetricGauge
  def mapGauge: MetricGauge
}
