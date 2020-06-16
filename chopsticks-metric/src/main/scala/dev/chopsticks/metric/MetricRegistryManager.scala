package dev.chopsticks.metric

import java.util.concurrent.atomic.AtomicReference

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio._

object MetricRegistryManager {
  trait Service[Id, C <: MetricGroup] {
    def activeMap: Map[Id, MetricRegistry.Service[C]]
    def createLayer(id: Id): ULayer[MetricRegistry[C]]
  }

  def live[Id: zio.Tag, C <: MetricGroup: zio.Tag]: URLayer[MetricRegistryFactory[C], MetricRegistryManager[Id, C]] = {
    ZLayer.fromEffect {
      for {
        factory <- ZIO.access[MetricRegistryFactory[C]](_.get)
      } yield {
        new Service[Id, C] {
          private val atomic = new AtomicReference(Map.empty[Id, MetricRegistry.Service[C]])

          override def activeMap: Map[Id, MetricRegistry.Service[C]] = atomic.get

          override def createLayer(id: Id): ULayer[MetricRegistry[C]] = {
            factory
              .fresh
              .build
              .map(_.get)
              .tap { registry =>
                ZManaged.fromEffect {
                  UIO {
                    atomic.getAndUpdate(_.updated(id, registry))
                  }
                }
              }
              .onExit {
                case Exit.Success(_) => UIO(atomic.getAndUpdate(_ - id))
                case Exit.Failure(_) => UIO.unit
              }
              .toLayer
          }
        }
      }
    }
  }
}
