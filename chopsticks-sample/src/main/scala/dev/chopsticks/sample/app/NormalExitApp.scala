package dev.chopsticks.sample.app

import akka.NotUsed
import akka.actor.Actor
import com.typesafe.config.Config
import dev.chopsticks.fp.AppLayer.AppEnv
import dev.chopsticks.fp.DiEnv.{DiModule, LiveDiEnv}
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.{AkkaDiApp, AppLayer, DiEnv, DiLayers}
import zio.{Task, ZIO}

object NormalExitApp extends AkkaDiApp[NotUsed] {
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
    ZIO.accessM[IzLogging](_.get.zioLogger.info("works"))
  }
}
