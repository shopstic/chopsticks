package dev.chopsticks.fp

import akka.actor.{typed, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.ExecutionContextExecutor
import akka.actor.typed.scaladsl.adapter._

trait AkkaEnv {
  def akkaService: AkkaEnv.Service
}

object AkkaEnv {
  trait Service {
    implicit def actorSystem: ActorSystem

    implicit lazy val typedActorSystem: typed.ActorSystem[Nothing] = actorSystem.toTyped
    implicit lazy val materializer: Materializer = ActorMaterializer()
    implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  }

  object Service {
    final case class Live(actorSystem: ActorSystem) extends Service
  }
}
