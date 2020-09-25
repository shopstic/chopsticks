package dev.chopsticks.metric

import dev.chopsticks.fp.util.{SharedResourceFactory, SharedResourceManager}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{RLayer, UManaged, ZLayer, ZManaged}

object MetricServiceManager {
  def live[Grp <: MetricGroup: zio.Tag, Cfg: zio.Tag, Svc: zio.Tag](
    serviceFactory: MetricServiceFactory[Grp, Cfg, Svc]
  ): RLayer[MetricRegistryFactory[Grp], MetricServiceManager[Cfg, Svc]] = {
    ZLayer.fromManagedMany {
      for {
        registryFactory <- ZManaged.access[MetricRegistryFactory[Grp]](_.get)
        factory = ZLayer.succeed {
          val result: SharedResourceFactory.Service[Any, Cfg, Svc] =
            new SharedResourceFactory.Service[Any, Cfg, Svc] {
              override def manage(id: Cfg): UManaged[Svc] = {
                registryFactory.fresh.build.map(_.get).map { registry =>
                  serviceFactory.create(registry, id)
                }
              }
            }
          result
        }
        manager <- SharedResourceManager.fromFactory(factory).build
      } yield manager
    }
  }
}
