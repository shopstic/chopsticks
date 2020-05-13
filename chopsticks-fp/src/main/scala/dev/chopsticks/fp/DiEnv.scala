package dev.chopsticks.fp

import distage.Injector
import izumi.distage.model.effect.DIEffect
import izumi.distage.model.exceptions.ProvisioningException
import izumi.distage.planning.extensions.GraphDumpBootstrapModule
import izumi.fundamentals.reflection.Tags.Tag
import zio.{Has, RIO, Task, UIO, ZIO}

object DiEnv {
  type DiModule = izumi.distage.model.definition.Module
  private val zioDiEffect = implicitly[DIEffect[Task]]

  object InterruptiableZioDiEffect extends DIEffect[Task] {
    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = zioDiEffect.flatMap[A, B](fa)(f)
    override def bracket[A, B](acquire: => Task[A])(release: A => Task[Unit])(use: A => Task[B]): Task[B] = {
      zioDiEffect.bracket[A, B](acquire)(release)(use)
    }

    override def bracketCase[A, B](
      acquire: => Task[A]
    )(release: (A, Option[Throwable]) => Task[Unit])(use: A => Task[B]): Task[B] = {
      // Make interruptible
      zioDiEffect.bracketCase(acquire.interruptible)(release)(use)
    }
    override def maybeSuspend[A](eff: => A): Task[A] = zioDiEffect.maybeSuspend(eff)
    override def definitelyRecover[A](action: => Task[A])(recover: Throwable => Task[A]): Task[A] =
      zioDiEffect.definitelyRecover(action)(recover)
    override def definitelyRecoverCause[A](
      action: => Task[A]
    )(recoverCause: (Throwable, () => Throwable) => Task[A]): Task[A] = {
      zioDiEffect.definitelyRecoverCause(action)(recoverCause)
    }
    override def fail[A](t: => Throwable): Task[A] = zioDiEffect.fail(t)
    override def pure[A](a: A): Task[A] = zioDiEffect.pure(a)
    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = zioDiEffect.map(fa)(f)
    override def map2[A, B, C](fa: Task[A], fb: Task[B])(f: (A, B) => C): Task[C] = zioDiEffect.map2(fa, fb)(f)
  }

  final case class LiveDiEnv[E <: Has[_]: Tag](env: DiModule) extends DiEnv[E] {
    override def run(app: RIO[E, Unit], dumpGraph: Boolean = false): Task[Int] = {
      implicit val diEff: DIEffect[Task] = InterruptiableZioDiEffect
      val injector = if (dumpGraph) Injector(GraphDumpBootstrapModule()) else Injector()

      injector
        .produceGetF[Task, E](env).toZIO
        .use { env =>
          app.provide(env)
            .ensuring(ZIO.interruptAllChildren)
            .as(0)
        }
        .catchSome {
          case e: ProvisioningException =>
            UIO {
              Console.err.println(e.getMessage)
              1
            }
        }
    }
  }
}

abstract class DiEnv[E <: Has[_]: Tag] {
  import DiEnv._
  def env: DiModule
  def run(app: RIO[E, Unit], dumpGraph: Boolean = false): Task[Int]
}
