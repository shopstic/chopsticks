package dev.chopsticks.sample.app

import akka.actor.ActorSystem
import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, ConfigEnv, ZLogger}
import dev.chopsticks.util.config.PureconfigLoader
import zio.{ZIO, ZManaged}

object PlainSampleApp extends AkkaApp {
  final case class AppConfig(foo: Int, bar: String)

  type Cfg = ConfigEnv[AppConfig]
  type Env = AkkaApp.Env with Cfg

  protected def createEnv(rawConfig: Config): ZManaged[AkkaApp.Env, Nothing, Env] = {
    import dev.chopsticks.util.config.PureconfigConverters._

    ZManaged
      .environment[AkkaApp.Env]
      .map { akkaEnv =>
        new AkkaApp.LiveEnv with Cfg {
          implicit val actorSystem: ActorSystem = akkaEnv.actorSystem
          val config: AppConfig = PureconfigLoader.unsafeLoad[AppConfig](rawConfig, "app")
        }
      }
  }

  protected def run: ZIO[Env, Nothing, Unit] = {
    for {
      config <- ZIO.access[Cfg](_.config)
      _ <- ZLogger.info(s"Works config=$config")
    } yield ()
  }
}
