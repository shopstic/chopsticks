package dev.chopsticks.metric.prom

import dev.chopsticks.metric.{MetricRegistry, MetricRegistryFactory}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import io.prometheus.client.CollectorRegistry
import zio.{Scope, ULayer, URIO, URLayer, ZIO, ZLayer}

object PromMetricRegistryFactory {
  def live[C <: MetricGroup: zio.Tag](prefix: String): URLayer[CollectorRegistry, MetricRegistryFactory[C]] = {
    val managed: URIO[CollectorRegistry, MetricRegistryFactory[C]] = for {
      collector <- ZIO.service[CollectorRegistry]
    } yield {
      new MetricRegistryFactory[C] {
        override def manage: URIO[Scope, MetricRegistry[C]] = {
          ZIO.acquireRelease {
            ZIO.succeed(new PromMetricRegistry[C](prefix, collector))
          } { registry =>
            ZIO.succeed(registry.removeAll())
          }
        }
      }
    }

    ZLayer(managed)
  }

  def live[C <: MetricGroup: zio.Tag](prefix: String, registry: CollectorRegistry): ULayer[MetricRegistryFactory[C]] = {
    ZLayer.succeed(registry) >>> live[C](prefix)
  }
}
