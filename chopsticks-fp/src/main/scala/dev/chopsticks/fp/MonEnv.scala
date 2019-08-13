package dev.chopsticks.fp

import com.typesafe.config.Config
import dev.chopsticks.fp.AkkaApp.Env
import kamon.Kamon
import zio.{Task, RIO}

trait MonEnv {
  def monitor(config: Config): RIO[AkkaApp.Env, Unit]
}

object MonEnv {
  trait Live extends MonEnv {
    def monitor(config: Config): RIO[Env, Unit] = {
      Task {
        Kamon.reconfigure(config)
        Kamon.loadModules()
      }
    }
  }
}
