package dev.chopsticks.fp

import akka.NotUsed
import akka.stream.scaladsl.{Flow, RunnableGraph}
import akka.stream.{Attributes, KillSwitch}
import zio._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
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

  @deprecated("Use graphM instead", "0.21.0")
  def graph[R, A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): RIO[AkkaEnv with LogEnv with R, A] = {
    graphM(make)
  }

  def graphM[R, A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): RIO[AkkaEnv with LogEnv with R, A] = {
    ZIO.accessM[AkkaEnv with LogEnv with R] { env =>
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

  @deprecated("Use interruptableGraphM instead", "0.21.0")
  def interruptableGraph[R, A](
    make: RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with LogEnv with R, A] = {
    interruptableGraphM(make, graceful)(ctx)
  }

  def interruptableGraphM[R, A](
    make: RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with LogEnv with R, A] = {
    ZIO.accessM[AkkaEnv with LogEnv with R] { env =>
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

  def interruptableMapAsyncM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZIO.access[AkkaEnv with R] { env =>
      implicit val ec: ExecutionContextExecutor = env.dispatcher

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

                env.unsafeRunToFuture(interruptableTask.provide(env)).flatten
              }
              .watchTermination() { (_, f) =>
                f.onComplete(_ => completionPromise.success(()))
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed))
    }
  }

  def interruptableMapAsync[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, Future[NotUsed]] = {
    env.unsafeRun(interruptableMapAsyncM(parallelism)(runTask).provide(env))
  }

  def interruptableMapAsyncUnorderedM[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZIO.access[AkkaEnv with R] { env =>
      implicit val ec: ExecutionContextExecutor = env.dispatcher

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

              env.unsafeRunToFuture(interruptableTask.provide(env)).flatten
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
  }

  def interruptableMapAsyncUnordered[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, Future[NotUsed]] = {
    env.unsafeRun(interruptableMapAsyncUnorderedM(parallelism, attributes)(runTask).provide(env))
  }

  def mapAsyncM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.access[AkkaEnv with R] { env =>
      Flow[A]
        .mapAsync(parallelism) { a =>
          env.unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
        }
    }
  }

  def mapAsync[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, NotUsed] = {
    env.unsafeRun(mapAsyncM(parallelism)(runTask).provide(env))
  }

  def mapAsyncUnorderedM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.access[AkkaEnv with R] { env =>
      Flow[A]
        .mapAsyncUnordered(parallelism) { a: A =>
          env.unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
        }
    }
  }

  def mapAsyncUnordered[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, NotUsed] = {
    env.unsafeRun(mapAsyncUnorderedM(parallelism)(runTask).provide(env))
  }

  private val ecTask = Task.descriptor.map(_.executor.asEC)

  def withEc[T](make: ExecutionContext => T): Task[T] = {
    ecTask.map(make)
  }
}
