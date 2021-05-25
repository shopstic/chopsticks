package dev.chopsticks.fp

import dev.chopsticks.fp.zio_ext._
import distage.Injector
import izumi.distage.model.effect.QuasiIO
import izumi.distage.model.exceptions.ProvisioningException
import izumi.distage.planning.extensions.GraphDumpBootstrapModule
import izumi.reflect.Tag
import zio._

object DiEnv {
  type DiModule = izumi.distage.model.definition.Module
  private val zioDiEffect = implicitly[QuasiIO[Task]]

  object InterruptiableZioDiEffect extends QuasiIO[Task] {

    override def maybeSuspendEither[A](eff: => Either[Throwable, A]): Task[A] = zioDiEffect.maybeSuspendEither(eff)

    override def redeem[A, B](action: => Task[A])(failure: Throwable => Task[B], success: A => Task[B]): Task[B] =
      zioDiEffect.redeem(action)(failure, success)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = zioDiEffect.flatMap(fa)(f)

    override def bracket[A, B](acquire: => Task[A])(release: A => Task[Unit])(use: A => Task[B]): Task[B] = {
      zioDiEffect.bracket[A, B](acquire.interruptible)(release)(use)
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

    override def definitelyRecoverCause[A](action: => Task[A])(recoverCause: (Throwable, () => Throwable) => Task[A])
      : Task[A] = zioDiEffect.definitelyRecoverCause(action)(recoverCause)

    override def fail[A](t: => Throwable): Task[A] = zioDiEffect.fail(t)

    override def pure[A](a: A): Task[A] = zioDiEffect.pure(a)

    override def map2[A, B, C](fa: Task[A], fb: => Task[B])(f: (A, B) => C): Task[C] = zioDiEffect.map2(fa, fb)(f)

    override def map[A, B](fa: Task[A])(f: A => B): Task[B] = zioDiEffect.map(fa)(f)
  }

  final case class LiveDiEnv[E <: Has[_]: Tag](env: DiModule) extends DiEnv[E] {
    override def build(dumpGraph: Boolean = false): TaskManaged[E] = {
      implicit val diEff: QuasiIO[Task] = InterruptiableZioDiEffect
      val injector = if (dumpGraph) Injector[Task](GraphDumpBootstrapModule()) else Injector[Task]()
      injector.produceGet[E](env).toZIO
    }

    override def run(app: RIO[E, Unit], dumpGraph: Boolean = false): Task[Int] = {
      build(dumpGraph)
        .use { env =>
          app
            .provide(env)
            .as(0)
            .interruptAllChildrenPar
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

abstract class DiEnv[E <: Has[_]] {
  import DiEnv._
  def env: DiModule
  def build(dumpGraph: Boolean = false): TaskManaged[E]
  def run(app: RIO[E, Unit], dumpGraph: Boolean = false): Task[Int]
}
