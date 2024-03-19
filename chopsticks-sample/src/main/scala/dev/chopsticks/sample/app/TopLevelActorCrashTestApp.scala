package dev.chopsticks.sample.app

import org.apache.pekko.actor.{Actor, Props}
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio.{RIO, Scope, ZIO}

object TopLevelActorCrashTestApp extends ZPekkoApp {
  final class TestActor extends Actor {
    override def receive: Receive = {
      case _ => throw new IllegalStateException("Test unexpected death")
    }
  }

  override def run: RIO[ZAkkaAppEnv with Scope, Any] = {
    app
  }

  //noinspection TypeAnnotation
  def app = {
    ZIO
      .acquireRelease(ZIO.succeed("foo")) { _ =>
        ZIO.succeed(
          println("This release should still be invoked even when the actor system is terminated unexpectedly")
        )
      }
      .zipRight {
        for {
          akkaSvc <- ZIO.service[PekkoEnv]
          actor <- ZIO.attempt {
            akkaSvc.actorSystem.actorOf(Props(new TestActor))
          }
          _ <- ZIO
            .attempt {
              actor ! "foo"
            }.delay(java.time.Duration.ofSeconds(2))
          _ <- ZIO.never.unit
        } yield ()
      }
  }
}
