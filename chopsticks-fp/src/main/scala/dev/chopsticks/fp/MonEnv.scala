package dev.chopsticks.fp

import com.typesafe.config.Config
import dev.chopsticks.fp.AkkaApp.Env
import kamon.Kamon
import kamon.system.SystemMetrics
import zio.{Task, TaskR}

trait MonEnv {
  def monitor(config: Config): TaskR[AkkaApp.Env, Unit]
}

object MonEnv {
  trait Live extends MonEnv {
    def monitor(config: Config): TaskR[Env, Unit] = {
      Task {
        Kamon.reconfigure(config)
        Kamon.loadReportersFromConfig()
        SystemMetrics.startCollecting()
      }
    }
  }
}
