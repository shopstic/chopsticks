package dev.chopsticks.stream

import dev.chopsticks.stream.ZAkkaScope.ZAkkaScopeNotReleasedAfterClosingDefect
import zio.{Exit, Fiber, IO, Scope, UIO, URIO, ZIO}

object ZAkkaScope {
  final case object ZAkkaScopeNotReleasedAfterClosingDefect
      extends IllegalStateException(
        "Stream scope is not released! This is likely due to the scope being extended, which should never be the case"
      )

  def make: UIO[ZAkkaScope] = Scope.make.map(scope => new ZAkkaScope(scope))
}

final class ZAkkaScope private (val underlying: Scope.Closeable) {
  def fork[R, E, V](effect: ZIO[R, E, V]): URIO[R, Fiber.Runtime[E, V]] = {
    effect.forkIn(underlying)
  }

  // todo remove it if not needed
//  def overrideForkScope[R, E, V](effect: ZIO[R, E, V]): ZIO[R, E, V] = {
//    effect.overrideForkScope(scope.scope)
//  }

  def close(): IO[ZAkkaScopeNotReleasedAfterClosingDefect.type, Unit] = {
    underlying.close(Exit.unit)
//    for {
//      _ <- scope.close(Exit.unit)
    // todo remove it if not needed
//      _ <- scope.
//        .flatMap { released =>
//          ZIO.die(ZAkkaScopeNotReleasedAfterClosingDefect).when(!released)
//        }
//        .when(!closed)
//    } yield ()
  }
}
