package dev.chopsticks.metric

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{ULayer, ZLayer}

import scala.annotation.nowarn

object MetricRegistryFactory {
  def apply[C <: MetricGroup](
    layer: ULayer[MetricRegistry[C]]
  )(implicit @nowarn _1: zio.Tag[C]): ULayer[MetricRegistryFactory[C]] = {
    ZLayer.succeed(layer)
  }
}
