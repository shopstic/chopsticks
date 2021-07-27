package dev.chopsticks.fp.akka_env

import akka.actor.CoordinatedShutdown.JvmExitReason
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{typed, ActorSystem, CoordinatedShutdown}
import com.typesafe.config.ConfigFactory
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
      // Shutting down of the ActorSystem should be via ZManaged release
      // ZAkkaApp handles shutdown properly, hence we want to disable Akka's own
      // shutdown hook here
      overriddenConfig = ConfigFactory
        .parseString(
          """
          |akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off""".stripMargin
        )
        .withFallback(hoconConfig)
      actorSystem <- ZManaged.make {
        Task {
          val as = ActorSystem(appName, overriddenConfig)
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
