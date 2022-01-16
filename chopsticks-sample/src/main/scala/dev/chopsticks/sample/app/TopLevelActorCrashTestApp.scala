package dev.chopsticks.sample.app

import akka.actor.{Actor, Props}
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.{ExitCode, RIO, Task, UIO, ZIO, ZManaged}

object TopLevelActorCrashTestApp extends ZAkkaApp {
  final class TestActor extends Actor {
    override def receive: Receive = {
      case _ => throw new IllegalStateException("Test unexpected death")
    }
  }

  override def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    app.as(ExitCode(0))
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
