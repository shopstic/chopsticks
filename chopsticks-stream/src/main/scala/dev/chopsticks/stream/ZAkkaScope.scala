package dev.chopsticks.stream

import dev.chopsticks.stream.ZAkkaScope.ZAkkaScopeNotReleasedAfterClosingDefect
import zio.{Exit, Fiber, IO, UIO, URIO, ZIO, ZScope}

object ZAkkaScope {
  final case object ZAkkaScopeNotReleasedAfterClosingDefect
      extends IllegalStateException(
        "Stream scope is not released! This is likely due to the scope being extended, which should never be the case"
      )

  def make: UIO[ZAkkaScope] = ZScope.make[Exit[Any, Any]].map(new ZAkkaScope(_))
}

final class ZAkkaScope private (scope: ZScope.Open[Exit[Any, Any]]) {
  def fork[R, E, V](effect: ZIO[R, E, V]): URIO[R, Fiber.Runtime[E, V]] = {
    effect.forkIn(scope.scope)
  }

  def overrideForkScope[R, E, V](effect: ZIO[R, E, V]): ZIO[R, E, V] = {
    effect.overrideForkScope(scope.scope)
  }

  def close(): IO[ZAkkaScopeNotReleasedAfterClosingDefect.type, Unit] = {
    for {
      closed <- scope.close(Exit.Success(()))
      _ <- scope.scope.released
        .flatMap { released =>
          ZIO.die(ZAkkaScopeNotReleasedAfterClosingDefect).when(!released)
        }
        .when(!closed)
    } yield ()
  }
}
