package dev.chopsticks.dstream

import dev.chopsticks.fp.akka_env.AkkaEnv
import izumi.reflect.Tag
import zio.clock.Clock
import zio.{UManaged, URLayer, ZManaged}

object DstreamStateFactory {

  trait Service {
    def createStateService[Req: Tag, Res: Tag](serviceId: String): UManaged[DstreamState.Service[Req, Res]]
  }

  def live: URLayer[AkkaEnv with Clock with DstreamStateMetricsManager, DstreamStateFactory] = {
    val managed = ZManaged.environment[AkkaEnv with Clock with DstreamStateMetricsManager].map { env =>
      new Service {
        override def createStateService[Req: Tag, Res: Tag](serviceId: String) = {
          DstreamState.managed[Req, Res](serviceId).provide(env)
        }
      }
    }
    managed.toLayer
  }

}
