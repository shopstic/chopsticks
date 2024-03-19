package dev.chopsticks.fp.pekko_env

import org.apache.pekko.actor.CoordinatedShutdown.JvmExitReason
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.{typed, ActorSystem, CoordinatedShutdown}
import com.typesafe.config.ConfigFactory
import dev.chopsticks.fp.config.HoconConfig
import dev.chopsticks.fp.zio_ext._
import zio.{RLayer, URIO, ZIO, ZLayer}

import scala.concurrent.ExecutionContextExecutor

trait PekkoEnv extends Serializable {
  implicit def actorSystem: ActorSystem
  implicit lazy val typedActorSystem: typed.ActorSystem[Nothing] = actorSystem.toTyped
  implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
}

object PekkoEnv extends Serializable {
  def actorSystem: URIO[PekkoEnv, ActorSystem] = ZIO.service[PekkoEnv].map(_.actorSystem)
  def dispatcher: URIO[PekkoEnv, ExecutionContextExecutor] = ZIO.service[PekkoEnv].map(_.dispatcher)

  final case class Live(actorSystem: ActorSystem) extends PekkoEnv

  val any: ZLayer[PekkoEnv, Nothing, PekkoEnv] = ZLayer.service[PekkoEnv]

  def live(implicit actorSystem: ActorSystem): ZLayer[Any, Nothing, PekkoEnv] = ZLayer.succeed(Live(actorSystem))

  def live(appName: String = "app"): RLayer[HoconConfig, PekkoEnv] = {
    val effect = for {
      hoconConfig <- HoconConfig.get
      // Shutting down of the ActorSystem should be via ZManaged release
      // ZPekkoApp handles shutdown properly, hence we want to disable Pekko's own
      // shutdown hook here
      overriddenConfig = ConfigFactory
        .parseString(
          """
          |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off""".stripMargin
        )
        .withFallback(hoconConfig)
      actorSystem <- ZIO.acquireRelease {
        ZIO.attempt {
          val as = ActorSystem(appName, overriddenConfig)
          val cs = CoordinatedShutdown(as)

          as -> cs
        }
      } { case (as, cs) =>
        (ZIO
          .fromFuture(_ => cs.run(JvmExitReason)).unit interruptibleRace ZIO
          .fromFuture { _ =>
            as.whenTerminated
          })
          .unit
          .orDie
      }
        .map(_._1)
    } yield Live(actorSystem)

    ZLayer.scoped(effect)
  }
}
