package dev.chopsticks.metric

import java.util.concurrent.atomic.AtomicReference

import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio._

abstract class MetricServiceManager[G <: MetricGroup: zio.Tag, Svc: zio.Tag](registryLayer: ULayer[MetricRegistry[G]])
    extends MetricServiceTracker[Svc] {
  protected val atomic = new AtomicReference(Map.empty[Any, (Svc, Int)])

  override def activeSet: Set[Svc] = atomic.get.values.map(_._1).toSet

  protected def create(id: Any)(createService: MetricRegistry.Service[G] => Svc): ZManaged[Any, Nothing, Svc] = {
    registryLayer
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
