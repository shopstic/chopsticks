package dev.chopsticks.fp

import akka.NotUsed
import akka.stream.scaladsl.{Flow, RunnableGraph}
import akka.stream.{Attributes, KillSwitch}
import dev.chopsticks.util.implicits.SquantsImplicits._
import squants.time.Nanoseconds
import zio._
import zio.clock.Clock

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ZIOExt {

  type MeasuredLogging = LogEnv with Clock

  object Implicits {
    implicit def scalaToZioDuration(d: Duration): zio.duration.Duration =
      zio.duration.Duration.fromScala(d)

    implicit final class TaskExtensions[A](io: Task[A]) {
      def logResult(name: String, result: A => String)(implicit ctx: LogCtx): TaskR[MeasuredLogging, A] = {
        new ZIOExtensions(io).logResult(name, result)
      }

      def log(name: String)(implicit ctx: LogCtx): TaskR[MeasuredLogging, A] = {
        new ZIOExtensions(io).log(name)
      }
    }

    implicit final class ZIOExtensions[R >: Nothing, E <: Any, A](io: ZIO[R, E, A]) {
      def logResult(name: String, result: A => String)(
        implicit ctx: LogCtx
      ): ZIO[R with MeasuredLogging, E, A] = {
        val start: ZIO[R with MeasuredLogging, E, Long] = nanoTime <* ZLogger.info(s"[$name] started")

        ZIO.bracketExit(start) { (startTime: Long, exit: Exit[E, A]) =>
          for {
            elapse <- nanoTime.map(endTime => endTime - startTime)
            formattedElapse = Nanoseconds(elapse).inBestUnit.rounded(2)
            _ <- exit.toEither match {
              case Left(FiberFailure(cause)) if cause.interrupted =>
                ZLogger.warn(s"[$name] [took $formattedElapse] interrupted")

              case Left(e) =>
                ZLogger.error(s"[$name] [took $formattedElapse] failed", e)

              case Right(r) =>
                ZLogger.info(s"[$name] [took $formattedElapse] ${result(r)}")
            }
          } yield ()
        }((_: Long) => io)
      }

      def log(name: String)(implicit ctx: LogCtx): ZIO[R with MeasuredLogging, E, A] = {
        logResult(name, _ => "completed")
      }
    }
  }

  def fromFutureWithEnv[R >: Nothing, A](make: (R, ExecutionContext) => Future[A]): TaskR[R, A] = {
    ZIO.accessM((env: R) => ZIO.fromFuture(ec => make(env, ec)))
  }

  def fromAkkaFuture[A](make: AkkaEnv => Future[A]): TaskR[AkkaEnv, A] = {
    ZIO.accessM[AkkaEnv] { env =>
      val f = make(env)
      f.value
        .fold(
          Task.effectAsync { cb: (Task[A] => Unit) =>
            f.onComplete {
              case Success(a) => cb(Task.succeed(a))
              case Failure(t) => cb(Task.fail(t))
            }(env.dispatcher)
          }
        )(Task.fromTry(_))
    }
  }

  def interruptableGraph[R >: Nothing, A](
    make: => TaskR[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): TaskR[R with AkkaEnv with LogEnv, A] = {
    ZIO.accessM[R with AkkaEnv with LogEnv] { env =>
      make.flatMap { graph =>
        val (ks, f) = graph.run()(env.materializer)
        val task = f.value
          .fold(
            Task.effectAsync { cb: (Task[A] => Unit) =>
              f.onComplete {
                case Success(a) => cb(Task.succeed(a))
                case Failure(t) => cb(Task.fail(t))
              }(env.dispatcher)
            }
          )(Task.fromTry(_))

        task.onInterrupt(
          UIO {
            if (graceful) ks.shutdown()
            else ks.abort(new InterruptedException("Stream (interruptableGraph) was interrupted"))
          } *> task.fold(
            e =>
              env.logger
                .error(s"Graph interrupted (graceful=$graceful) which resulted in exception: ${e.getMessage}", e),
            _ => ()
          )
        )
      }
    }
  }

  def interruptableMapAsync[A, B, Env <: AkkaEnv](
    parallelism: Int
  )(runTask: A => TaskR[Env, B])(implicit env: Env): Flow[A, B, Future[NotUsed]] = {
    import env._

    Flow
      .lazyInitAsync(() => {
        val completionPromise = Promise[Unit]()
        val interruptTask = Task.fromFuture(_ => completionPromise.future)
        Future.successful(
          Flow[A]
            .mapAsync(parallelism) { a =>
              val interruptableTask = for {
                fib <- runTask(a).fold(Future.failed, Future.successful).fork
                c <- (interruptTask *> fib.interrupt).fork
                ret <- fib.join
                _ <- c.interrupt.fork
              } yield ret

              unsafeRunToFuture(interruptableTask.provide(env)).flatten
            }
            .watchTermination() { (_, f) =>
              f.onComplete(_ => completionPromise.success(()))
              NotUsed
            }
        )
      })
      .mapMaterializedValue(_.map(_ => NotUsed))
  }

  def interruptableMapAsyncUnordered[R <: AkkaEnv, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => TaskR[R, B])(implicit env: R): Flow[A, B, Future[NotUsed]] = {
    import env._

    Flow
      .lazyInitAsync(() => {
        val completionPromise = Promise[Unit]()
        val interruptTask = Task.fromFuture(_ => completionPromise.future)
        val flow = Flow[A]
          .mapAsyncUnordered(parallelism) { a =>
            val interruptableTask = for {
              fib <- runTask(a).fold(Future.failed, Future.successful).fork
              c <- (interruptTask *> fib.interrupt).fork
              ret <- fib.join
              _ <- c.interrupt.fork
            } yield ret

            unsafeRunToFuture(interruptableTask.provide(env)).flatten
          }

        val flowWithAttrs = attributes.fold(flow)(attrs => flow.withAttributes(attrs))

        Future.successful(
          flowWithAttrs
            .watchTermination() { (_, f) =>
              f.onComplete(_ => completionPromise.success(()))
              NotUsed
            }
        )
      })
      .mapMaterializedValue(_.map(_ => NotUsed))
  }

  def mapAsync[A, B, E <: AkkaEnv](
    parallelism: Int
  )(runTask: A => TaskR[E, B])(implicit env: E): Flow[A, B, NotUsed] = {
    import env._

    Flow[A]
      .mapAsync(parallelism) { a =>
        unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
      }
  }

  def mapAsyncUnordered[A, B, E <: AkkaEnv](
    parallelism: Int
  )(runTask: A => TaskR[E, B])(implicit env: E): Flow[A, B, NotUsed] = {
    import env._

    Flow[A]
      .mapAsyncUnordered(parallelism) { a =>
        unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
      }
  }

  private val ecTask = Task.descriptor.map(_.executor.asEC)

  def withEc[T](make: ExecutionContext => T): Task[T] = {
    ecTask.map(make)
  }

  private val nanoTime = ZIO.accessM((e: Clock) => e.clock.nanoTime)
}
