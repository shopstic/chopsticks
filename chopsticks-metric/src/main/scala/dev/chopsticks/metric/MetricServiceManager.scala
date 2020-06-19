package dev.chopsticks.metric

import java.util.concurrent.atomic.AtomicReference

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio._

object MetricServiceManager {
  type MetricId = Map[String, String]

  trait Service[C <: MetricGroup, Svc] {
    def activeSet: Set[Svc]
    def manage(id: Any)(createService: MetricRegistry.Service[C] => Svc): ZManaged[Any, Nothing, Svc]
  }

  def live[C <: MetricGroup: zio.Tag, Svc: zio.Tag]: URLayer[MetricRegistryFactory[C], MetricServiceManager[C, Svc]] = {
    val managed = for {
      factory <- ZManaged.access[MetricRegistryFactory[C]](_.get)
    } yield {
      new Service[C, Svc] {
        private val atomic = new AtomicReference(Map.empty[Any, (Svc, Int)])

        override def activeSet: Set[Svc] = atomic.get.values.map(_._1).toSet

        override def manage(id: Any)(createService: MetricRegistry.Service[C] => Svc): ZManaged[Any, Nothing, Svc] = {
          factory
            .fresh
            .build
            .map(_.get)
            .map { registry =>
              val state = atomic.updateAndGet { map =>
                map.get(id) match {
                  case Some((svc, count)) => map.updated(id, svc -> (count + 1))
                  case None => map.updated(id, createService(registry) -> 1)
                }
              }

              state(id)._1
            }
            .onExit {
              case Exit.Success(_) =>
                UIO {
                  atomic.updateAndGet { map =>
                    map.get(id) match {
                      case Some((_, 1)) => map - id
                      case Some((svc, count)) => map.updated(id, svc -> (count - 1))
                      case None => map
                    }
                  }
                }
                  .unit
              case Exit.Failure(_) => UIO.unit
            }
        }
      }
    }

    managed.toLayer
  }
}
