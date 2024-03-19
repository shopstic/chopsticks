package dev.chopsticks.metric

import dev.chopsticks.fp.util.{SharedResourceFactory, SharedResourceManager}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{RLayer, ZIO, ZLayer}

object MetricServiceManager {
  def live[Grp <: MetricGroup: zio.Tag, Cfg: zio.Tag, Svc: zio.Tag](
    serviceFactory: MetricServiceFactory[Grp, Cfg, Svc]
  ): RLayer[MetricRegistryFactory[Grp], MetricServiceManager[Cfg, Svc]] = {
    val effect = for {
      registryFactory <- ZIO.service[MetricRegistryFactory[Grp]]
      factory = ZLayer.succeed {
        val result: SharedResourceFactory[Any, Cfg, Svc] = (id: Cfg) => {
          registryFactory.manage.map { registry =>
            serviceFactory.create(registry, id)
          }
        }

        result
      }
      manager <- SharedResourceManager.fromFactory(factory).build.map(_.get)
    } yield manager

    ZLayer.scoped(effect)
  }
}
