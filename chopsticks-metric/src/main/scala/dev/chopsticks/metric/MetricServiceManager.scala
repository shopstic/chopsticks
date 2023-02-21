package dev.chopsticks.metric

import dev.chopsticks.fp.util.{SharedResourceFactory, SharedResourceManager}
import dev.chopsticks.metric.MetricRegistry.MetricGroup
import zio.{RLayer, Scope, ZIO, ZLayer}

object MetricServiceManager {
  def live[Grp <: MetricGroup: zio.Tag, Cfg: zio.Tag, Svc: zio.Tag](
    serviceFactory: MetricServiceFactory[Grp, Cfg, Svc]
  ): RLayer[MetricRegistryFactory[Grp], MetricServiceManager[Cfg, Svc]] = {
    ZLayer.scoped.apply {
      for {
        registryFactory <- ZIO.service[MetricRegistryFactory[Grp]]
        factory = ZLayer.succeed {
          new SharedResourceFactory[Any, Cfg, Svc]:
            override def manage(id: Cfg): ZIO[Scope, Nothing, Svc] =
              registryFactory.manage.map(registry => serviceFactory.create(registry, id))
        }
        manager <- SharedResourceManager.fromFactory(factory).build.map(_.get)
      } yield manager
    }
  }
}
