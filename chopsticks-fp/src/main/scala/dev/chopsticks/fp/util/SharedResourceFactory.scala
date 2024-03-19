package dev.chopsticks.fp.util

import zio.{Scope, ZIO}

trait SharedResourceFactory[R, Id, Res] {
  def manage(id: Id): ZIO[Scope with R, Nothing, Res]
}
