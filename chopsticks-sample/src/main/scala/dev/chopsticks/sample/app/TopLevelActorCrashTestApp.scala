package dev.chopsticks.sample.app

import akka.NotUsed
import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.{Task, UIO, ZIO, ZManaged}

object TopLevelActorCrashTestApp extends AkkaDiApp[NotUsed] {
  final class TestActor extends Actor {
    override def receive: Receive = {
      case _ => throw new IllegalStateException("Test unexpected death")
    }
  }

  override def config(allConfig: Config): Task[NotUsed] = Task.unit.as(NotUsed)

  override def liveEnv(akkaAppDi: DiModule, appConfig: NotUsed, allConfig: Config): Task[DiEnv[AppEnv]] = {
    Task {
      LiveDiEnv(
        akkaAppDi ++ DiLayers(
          AppLayer(app)
        )
      )
    }
  }

  //noinspection TypeAnnotation
  def app = {
    val managed = ZManaged.make(UIO("foo")) { _ =>
      UIO(println("This release should still be invoked even when the actor system is terminated unexpectedly"))
    }

    managed.use { _ =>
      for {
        akkaSvc <- ZIO.access[AkkaEnv](_.get)
        actor <- Task {
          akkaSvc.actorSystem.actorOf(Props(new TestActor))
        }
        _ <- Task {
          actor ! "foo"
        }.delay(java.time.Duration.ofSeconds(2))
        _ <- Task.never.unit
      } yield ()
    }
  }
}
