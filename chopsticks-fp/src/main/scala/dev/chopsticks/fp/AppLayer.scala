package dev.chopsticks.fp

import zio._

object AppLayer {
  type AppEnv = Has[Task[Unit]]

  def apply[R](rio: RIO[R, Unit]): URLayer[R, AppEnv] = {
    ZLayer.requires[R].map { env =>
      Has(rio.provide(env))
    }
  }
}
