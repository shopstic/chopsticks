package dev.chopsticks.fp.akka_env

import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{typed, ActorSystem, CoordinatedShutdown}
import dev.chopsticks.fp.config.HoconConfig
import zio.{RLayer, Task, URIO, ZIO, ZLayer, ZManaged}

import scala.concurrent.ExecutionContextExecutor

object AkkaEnv extends Serializable {
  trait Service extends Serializable {
    implicit def actorSystem: ActorSystem
    implicit lazy val typedActorSystem: typed.ActorSystem[Nothing] = actorSystem.toTyped
    implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  }

  def actorSystem: URIO[AkkaEnv, ActorSystem] = ZIO.access[AkkaEnv](_.get.actorSystem)
  def dispatcher: URIO[AkkaEnv, ExecutionContextExecutor] = ZIO.access[AkkaEnv](_.get.dispatcher)

  final case class Live(actorSystem: ActorSystem) extends Service

  val any: ZLayer[AkkaEnv, Nothing, AkkaEnv] = ZLayer.requires[AkkaEnv]

  def live(implicit actorSystem: ActorSystem): ZLayer[Any, Nothing, AkkaEnv] = ZLayer.succeed(Live(actorSystem))

  def live(appName: String = "app"): RLayer[HoconConfig, AkkaEnv] = {
    val managed = for {
      hoconConfig <- HoconConfig.get.toManaged_
      _ <- ZManaged.effect {
        if (!hoconConfig.getBoolean("akka.coordinated-shutdown.run-by-jvm-shutdown-hook")) {
          throw new IllegalArgumentException(
            "'akka.coordinated-shutdown.run-by-jvm-shutdown-hook' is not set to 'on'. Check your HOCON application config."
          )
        }
      }
      actorSystem <- ZManaged.make {
        Task {
          val as = ActorSystem(appName, hoconConfig)
          val cs = CoordinatedShutdown(as)

          as -> cs
        }
      } { case (as, cs) =>
        (Task
          .fromFuture(_ => cs.run(JvmExitReason)).unit raceFirst Task
          .fromFuture { _ =>
            as.whenTerminated
          })
          .unit
          .orDie
      }
        .map(_._1)
    } yield Live(actorSystem)

    managed.toLayer
  }
}
