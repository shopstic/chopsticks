package com.shopstic.fp

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import scalaz.zio.internal.PlatformLive
import scalaz.zio.{IO, Runtime, Task}

import scala.concurrent.{ExecutionContextExecutor, Future}

trait AkkaEnv {
  implicit def actorSystem: ActorSystem

  implicit lazy val materializer: Materializer = ActorMaterializer()
  implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  protected lazy val rt = Runtime[Any]((), PlatformLive.fromExecutionContext(dispatcher))

  def unsafeRun[E, A](zio: IO[E, A]): A = {
    rt.unsafeRun(zio)
  }

  def unsafeRunToFuture[A](task: Task[A]): Future[A] = {
    rt.unsafeRunToFuture(task)
  }
}
