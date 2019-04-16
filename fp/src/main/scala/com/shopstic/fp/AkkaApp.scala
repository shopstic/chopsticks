package com.shopstic.fp
import scalaz.zio.blocking.Blocking
import scalaz.zio.clock.Clock
import scalaz.zio.console.Console
import scalaz.zio.internal.{Platform, PlatformLive}
import scalaz.zio.random.Random
import scalaz.zio.system
import scalaz.zio.system.System

object AkkaApp {
  type Env = Clock with Console with system.System with Random with Blocking with AkkaEnv with LogEnv
  trait LiveEnv
      extends Clock.Live
      with Console.Live
      with System.Live
      with Random.Live
      with Blocking.Live
      with LogEnv.Live
      with AkkaEnv {
//    override val blocking: Blocking.Service[Any] = new Blocking.Service[Any] {
//      lazy val blockingExecutor: ZIO[Any, Nothing, Executor] = UIO(
//        Executor.fromExecutionContext(Int.MaxValue)(
//          actorSystem.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
//        )
//      )
//    }
  }
}

trait AkkaApp[R <: AkkaEnv] extends AppRuntime[R] {
  implicit val logCtx: LogCtx = LogCtx(getClass.getName)

  val Platform: Platform = PlatformLive.fromExecutionContext(Environment.dispatcher)
}
