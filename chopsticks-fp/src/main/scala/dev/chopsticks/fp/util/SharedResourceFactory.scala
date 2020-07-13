package dev.chopsticks.fp.util

import zio.ZManaged

object SharedResourceFactory {
  trait Service[R, Id, Res] {
    def manage(id: Id): ZManaged[R, Nothing, Res]
  }
}
