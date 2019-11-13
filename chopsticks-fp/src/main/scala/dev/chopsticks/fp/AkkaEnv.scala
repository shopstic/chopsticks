package dev.chopsticks.fp

import akka.actor.typed.scaladsl.adapter._
import akka.actor.{typed, ActorSystem}

import scala.concurrent.ExecutionContextExecutor

trait AkkaEnv {
  def akkaService: AkkaEnv.Service
}

object AkkaEnv {
  trait Service {
    implicit def actorSystem: ActorSystem

    implicit lazy val typedActorSystem: typed.ActorSystem[Nothing] = actorSystem.toTyped
    implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  }

  object Service {
    final case class Live(actorSystem: ActorSystem) extends Service
  }
}
