package dev.chopsticks.fp

import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ZPekko {
  def fromFutureWithEnv[R >: Nothing: Tag, A](make: (R, ExecutionContext) => Future[A]): RIO[R, A] = {
    ZIO.environmentWithZIO[R](env => ZIO.fromFuture(ec => make(env.get[R], ec)))
  }

  def fromPekkoFuture[A](make: PekkoEnv => Future[A]): RIO[PekkoEnv, A] = {
    ZIO.service[PekkoEnv].flatMap { pekkoService =>
      val f = make(pekkoService)
      f.value
        .fold(
          ZIO.async { cb: (Task[A] => Unit) =>
            f.onComplete {
              case Success(a) => cb(ZIO.succeed(a))
              case Failure(t) => cb(ZIO.fail(t))
            }(pekkoService.dispatcher)
          }
        )(ZIO.fromTry(_))
    }
  }
}
