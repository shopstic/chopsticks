package dev.chopsticks.fp

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import scalaz.zio.internal.PlatformLive
import scalaz.zio.{Exit, IO, Runtime, Task, ZIO}

import scala.concurrent.{ExecutionContextExecutor, Future}

trait AkkaEnv {
  implicit def actorSystem: ActorSystem

  implicit lazy val materializer: Materializer = ActorMaterializer()
  implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  protected lazy val rt = Runtime[Any]((), PlatformLive.fromExecutionContext(dispatcher))

  def unsafeRunSync[E, A](zio: ZIO[Any, E, A]): Exit[E, A] = {
    rt.unsafeRunSync(zio)
  }

  def unsafeRun[E, A](zio: IO[E, A]): A = {
    rt.unsafeRun(zio)
  }

  def unsafeRunToFuture[A](task: Task[A]): Future[A] = {
    rt.unsafeRunToFuture(task)
  }
}
