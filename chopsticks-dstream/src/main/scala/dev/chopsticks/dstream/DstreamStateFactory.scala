package dev.chopsticks.dstream

import dev.chopsticks.fp.akka_env.AkkaEnv
import izumi.reflect.Tag
import zio.clock.Clock
import zio.{UManaged, ZLayer, ZManaged}

object DstreamStateFactory {

  trait Service {
    def createStateService[Req: Tag, Res: Tag](metrics: DstreamStateMetrics): UManaged[DstreamState.Service[Req, Res]]
  }

  def live: ZLayer[AkkaEnv with Clock, Nothing, DstreamStateFactory] = {
    val managed = ZManaged.environment[AkkaEnv with Clock].map { env =>
      new Service {
        override def createStateService[Req: Tag, Res: Tag](metrics: DstreamStateMetrics) = {
          DstreamState.managed[Req, Res](metrics).provide(env)
        }
      }
    }
    managed.toLayer
  }

}
