package dev.chopsticks.metric

import dev.chopsticks.metric.MetricConfigs.{LabelNames, LabelValues, MetricLabel}
import dev.chopsticks.metric.MetricFactory.MetricGroup
import zio.{Has, UIO, ULayer, ZLayer}

import scala.annotation.nowarn

abstract class LabelledMetricsFactory[ML <: MetricLabel](@nowarn labelNames: LabelNames[ML]) {
  type Labels = ML
  type Underlying <: Has[_]
  type Group <: MetricGroup

  trait Service {
    def create(labelValues: LabelValues[Labels]): UIO[Underlying]
  }

  def factory(prefix: String, labelValues: LabelValues[Labels]): UIO[Underlying]

  def live(prefix: String): ULayer[Has[Service]] = {
    ZLayer.fromEffect(UIO {
      (labelValues: LabelValues[Labels]) =>
        {
          factory(prefix, labelValues)
        }
    })
  }
}
