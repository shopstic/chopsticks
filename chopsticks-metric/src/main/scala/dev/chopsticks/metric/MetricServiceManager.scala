package dev.chopsticks.metric

import java.util.concurrent.atomic.AtomicReference

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio._

object MetricServiceManager {
  type MetricId = Map[String, String]

  trait Service[C <: MetricGroup, Svc] {
    def activeSet: Set[Svc]
    def manage(createService: MetricRegistry.Service[C] => Svc): ZManaged[Any, Nothing, Svc]
  }

  def live[C <: MetricGroup: zio.Tag, Svc: zio.Tag]: URLayer[MetricRegistryFactory[C], MetricServiceManager[C, Svc]] = {
    ZLayer.fromEffect {
      for {
        factory <- ZIO.access[MetricRegistryFactory[C]](_.get)
      } yield {
        new Service[C, Svc] {
          private val atomic = new AtomicReference(Set.empty[Svc])

          override def activeSet: Set[Svc] = atomic.get

          override def manage(createService: MetricRegistry.Service[C] => Svc): ZManaged[Any, Nothing, Svc] = {
            factory
              .fresh
              .build
              .map(_.get)
              .map(createService)
              .tap { svc =>
                ZManaged.fromEffect {
                  UIO {
                    atomic.getAndUpdate(_ + svc)
                  }
                }
              }
              .onExit {
                case Exit.Success(svc) => UIO(atomic.getAndUpdate(_ - svc))
                case Exit.Failure(_) => UIO.unit
              }
          }
        }
      }
    }
  }
}
