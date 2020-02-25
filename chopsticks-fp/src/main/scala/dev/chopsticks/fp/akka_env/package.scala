package dev.chopsticks.fp

import akka.actor.{typed, ActorSystem}
import zio.{Has, ZLayer}
import akka.actor.typed.scaladsl.adapter._

import scala.concurrent.ExecutionContextExecutor

package object akka_env {
  type AkkaEnv = Has[AkkaEnv.Service]

  object AkkaEnv extends Serializable {
    trait Service extends Serializable {
      implicit def actorSystem: ActorSystem
      implicit lazy val typedActorSystem: typed.ActorSystem[Nothing] = actorSystem.toTyped
      implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
    }

    final case class Live(actorSystem: ActorSystem) extends Service

    val any: ZLayer[AkkaEnv, Nothing, AkkaEnv] = ZLayer.requires[AkkaEnv]

    def live(implicit actorSystem: ActorSystem): ZLayer.NoDeps[Nothing, AkkaEnv] = ZLayer.succeed(Live(actorSystem))
  }
}
