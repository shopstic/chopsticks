package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, ZService}
import zio.clock.Clock
import zio.console.Console
import zio.{Fiber, Has, RIO, Schedule, UIO, ZLayer, ZManaged}
import zio.duration._

object ZioInterruptionTestApp2 extends AkkaApp {

  override type Env = AkkaApp.Env with MyTest

  type MyTest = Has[Fiber.Runtime[Nothing, Int]]

  object MyTest {
    def live: ZLayer[Clock with Console, Nothing, Has[Fiber.Runtime[Nothing, Int]]] = {
      ZLayer.fromManaged(
        ZManaged.makeInterruptible {
          for {
            fib <- zio.console.putStrLn("here").repeat(Schedule.fixed(1.second)).forkDaemon
          } yield fib
        } { fib =>
          UIO(println("========== GOING TO INTERRUPT ===============")) *>
            fib.interrupt
        }
      )

    }
  }

  override protected def createEnv(untypedConfig: Config) = {
    ZLayer.requires[AkkaApp.Env] ++ MyTest.live
  }

  override def run: RIO[Env, Unit] = {
    for {
      _ <- ZService[Fiber.Runtime[Nothing, Int]]
    } yield {
      println("BODY")
    }
  }
}
