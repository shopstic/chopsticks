package dev.chopsticks.dstream

import dev.chopsticks.dstream.metric.DstreamStateMetricsManager
import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio.{Scope, Tag, URIO, URLayer, ZIO, ZLayer}

trait DstreamStateFactory {
  def manage[Req: Tag, Res: Tag](serviceId: String): URIO[Scope, DstreamState[Req, Res]]
}

object DstreamStateFactory {

  def live: URLayer[PekkoEnv with DstreamStateMetricsManager, DstreamStateFactory] = {
    val managed =
      for {
        pekkoSvc <- ZIO.service[PekkoEnv]
        metricsManager <- ZIO.service[DstreamStateMetricsManager]
      } yield new DstreamStateFactory {
        override def manage[Req: Tag, Res: Tag](serviceId: String): URIO[Scope, DstreamState[Req, Res]] = {
          DstreamState
            .manage[Req, Res](serviceId)
            .provideSomeEnvironment[Scope](scopeEnv => scopeEnv.add(pekkoSvc).add(metricsManager))
        }
      }

    ZLayer(managed)
  }

}
