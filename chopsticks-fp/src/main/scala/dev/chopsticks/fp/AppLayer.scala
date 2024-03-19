package dev.chopsticks.fp

import zio._

object AppLayer {
  type AppEnv = Task[Unit]

  def apply[R](rio: RIO[R, Unit]): URLayer[R, AppEnv] = {
    ZLayer {
      ZIO.environment[R].map(env =>
        rio.provideEnvironment(env)
      )
    }
  }
}
