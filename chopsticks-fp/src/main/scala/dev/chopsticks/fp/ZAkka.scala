package dev.chopsticks.fp

import dev.chopsticks.fp.akka_env.AkkaEnv
import zio._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ZAkka {
  def fromFutureWithEnv[R >: Nothing, A](make: (R, ExecutionContext) => Future[A]): RIO[R, A] = {
    ZIO.accessM((env: R) => ZIO.fromFuture(ec => make(env, ec)))
  }

  def fromAkkaFuture[A](make: AkkaEnv.Service => Future[A]): RIO[AkkaEnv, A] = {
    ZIO.access[AkkaEnv](_.get).flatMap { akkaService =>
      val f = make(akkaService)
      f.value
        .fold(
          Task.effectAsync { cb: (Task[A] => Unit) =>
            f.onComplete {
              case Success(a) => cb(Task.succeed(a))
              case Failure(t) => cb(Task.fail(t))
            }(akkaService.dispatcher)
          }
        )(Task.fromTry(_))
    }
  }
}
