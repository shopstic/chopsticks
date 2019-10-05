package dev.chopsticks.fp

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import zio.internal.PlatformLive
import zio.{CancelableFuture, Exit, IO, Runtime, Task, ZIO}

import scala.concurrent.ExecutionContextExecutor

trait AkkaEnv {
  def akkaService: AkkaEnv.Service
}

object AkkaEnv {
  trait Service {
    implicit def actorSystem: ActorSystem

    implicit lazy val materializer: Materializer = ActorMaterializer()
    implicit lazy val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
    lazy val rt: Runtime[Any] = Runtime[Any](this, PlatformLive.fromExecutionContext(dispatcher))

    def unsafeRunSync[E, A](zio: ZIO[Any, E, A]): Exit[E, A] = {
      rt.unsafeRunSync(zio)
    }

    def unsafeRun[E, A](zio: IO[E, A]): A = {
      rt.unsafeRun(zio)
    }

    def unsafeRunToFuture[A](task: Task[A]): CancelableFuture[Throwable, A] = {
      rt.unsafeRunToFuture(task)
    }
  }

  object Service {
    def fromActorSystem(system: ActorSystem): Service = new Service {
      implicit val actorSystem: ActorSystem = system
    }
  }
}
