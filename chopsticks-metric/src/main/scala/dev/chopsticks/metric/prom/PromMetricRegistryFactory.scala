package dev.chopsticks.metric.prom

import dev.chopsticks.metric.{MetricRegistry, MetricRegistryFactory}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import io.prometheus.client.CollectorRegistry
import zio.{Has, UIO, ULayer, UManaged, URLayer, URManaged, ZLayer, ZManaged}

object PromMetricRegistryFactory {
  def live[C <: MetricGroup: zio.Tag](prefix: String): URLayer[Has[CollectorRegistry], MetricRegistryFactory[C]] = {
    val managed: URManaged[Has[CollectorRegistry], MetricRegistryFactory.Service[C]] = for {
      collector <- ZManaged.service[CollectorRegistry]
    } yield {
      new MetricRegistryFactory.Service[C] {
        override def manage: UManaged[MetricRegistry.Service[C]] = {
          ZManaged.make {
            UIO(new PromMetricRegistry[C](prefix, collector))
          } { registry =>
            UIO(registry.removeAll())
          }
        }
      }
    }

    managed.toLayer
  }

  def live[C <: MetricGroup: zio.Tag](prefix: String, registry: CollectorRegistry): ULayer[MetricRegistryFactory[C]] = {
    ZLayer.succeed(registry) >>> live[C](prefix)
  }
}
