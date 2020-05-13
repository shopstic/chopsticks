package dev.chopsticks.sample.app

import com.typesafe.config.Config
import dev.chopsticks.fp.{AkkaApp, AkkaDistageApp, ZService}
import distage.{Injector, ModuleDef}
import izumi.distage.model.definition
import izumi.distage.model.definition.DIResource.DIResourceBase
import izumi.distage.model.effect.DIEffect
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio._

object ZioInterruptionTestApp extends AkkaDistageApp {

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

  private val diEffect = implicitly[DIEffect[Task]]
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override protected def defineEnv(akkaAppModuleDef: definition.Module, lbConfig: Config): DIResourceBase[Task, Env] = {
    val module = akkaAppModuleDef ++ new ModuleDef {
      make[Fiber.Runtime[Nothing, Int]].fromHas(MyTest.live)
      make[Env].fromHas(ZIO.environment[Env])
    }

    implicit val eff: DIEffect[Task] = new DIEffect[Task] {
      override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = diEffect.flatMap[A, B](fa)(f)
      override def bracket[A, B](acquire: => Task[A])(release: A => Task[Unit])(use: A => Task[B]): Task[B] = {
        diEffect.bracket[A, B](acquire)(release)(use)
      }

      override def bracketCase[A, B](
        acquire: => Task[A]
      )(release: (A, Option[Throwable]) => Task[Unit])(use: A => Task[B]): Task[B] = {
        println(s"bracketCase $acquire")
        diEffect.bracketCase(acquire.interruptible)(release)(use)
      }
      override def maybeSuspend[A](eff: => A): Task[A] = diEffect.maybeSuspend(eff)
      override def definitelyRecover[A](action: => Task[A])(recover: Throwable => Task[A]): Task[A] =
        diEffect.definitelyRecover(action)(recover)
      override def definitelyRecoverCause[A](
        action: => Task[A]
      )(recoverCause: (Throwable, () => Throwable) => Task[A]): Task[A] = {
        diEffect.definitelyRecoverCause(action)(recoverCause)
      }
      override def fail[A](t: => Throwable): Task[A] = diEffect.fail(t)
      override def pure[A](a: A): Task[A] = diEffect.pure(a)
      override def map[A, B](fa: Task[A])(f: A => B): Task[B] = diEffect.map(fa)(f)
      override def map2[A, B, C](fa: Task[A], fb: Task[B])(f: (A, B) => C): Task[C] = diEffect.map2(fa, fb)(f)
    }

    Injector().produceGetF[Task, Env](module)
  }

  override protected def run: ZIO[Env, Throwable, Unit] = {
    for {
      fib <- ZService[Fiber.Runtime[Nothing, Int]]
    } yield {
      println("BODY")
    }
  }
}
