package dev.chopsticks.fp

import akka.NotUsed
import akka.stream.scaladsl.{Flow, RunnableGraph}
import akka.stream.{Attributes, KillSwitch}
import zio._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object ZAkka {

  def fromFutureWithEnv[R >: Nothing, A](make: (R, ExecutionContext) => Future[A]): RIO[R, A] = {
    ZIO.accessM((env: R) => ZIO.fromFuture(ec => make(env, ec)))
  }

  def fromAkkaFuture[A](make: AkkaEnv => Future[A]): RIO[AkkaEnv, A] = {
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

  def graph[R >: Nothing, A](
    make: => RIO[R, RunnableGraph[Future[A]]]
  ): RIO[R with AkkaEnv with LogEnv, A] = {
    ZIO.accessM[R with AkkaEnv with LogEnv] { env =>
      make.flatMap { graph =>
        val f = graph.run()(env.materializer)
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
  }

  def interruptableGraph[R >: Nothing, A](
    make: => RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[R with AkkaEnv with LogEnv, A] = {
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
  )(runTask: A => RIO[Env, B])(implicit env: Env): Flow[A, B, Future[NotUsed]] = {
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
  )(runTask: A => RIO[R, B])(implicit env: R): Flow[A, B, Future[NotUsed]] = {
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
  )(runTask: A => RIO[E, B])(implicit env: E): Flow[A, B, NotUsed] = {
    import env._

    Flow[A]
      .mapAsync(parallelism) { a =>
        unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
      }
  }

  def mapAsyncUnordered[A, B, E <: AkkaEnv](
    parallelism: Int
  )(runTask: A => RIO[E, B])(implicit env: E): Flow[A, B, NotUsed] = {
    import env._

    Flow[A]
      .mapAsyncUnordered(parallelism) { a: A =>
        unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
      }
  }

  private val ecTask = Task.descriptor.map(_.executor.asEC)

  def withEc[T](make: ExecutionContext => T): Task[T] = {
    ecTask.map(make)
  }
}
