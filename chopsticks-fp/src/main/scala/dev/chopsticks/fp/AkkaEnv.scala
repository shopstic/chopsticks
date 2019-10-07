package dev.chopsticks.fp

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.ExecutionContextExecutor

trait AkkaEnv {
  def akkaService: AkkaEnv.Service
}

object AkkaEnv {
  trait Service {
    implicit def actorSystem: ActorSystem

    implicit lazy val materializer: Materializer = ActorMaterializer()
    implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  }

  object Service {
    final case class Live(actorSystem: ActorSystem) extends Service
  }
}
