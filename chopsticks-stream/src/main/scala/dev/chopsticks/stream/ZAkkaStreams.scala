package dev.chopsticks.stream

import akka.NotUsed
import akka.actor.Status
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Source}
import akka.stream.{Attributes, KillSwitch, KillSwitches, OverflowStrategy}
import dev.chopsticks.fp.{AkkaEnv, LogCtx, LogEnv}
import zio.{RIO, Task, UIO, ZIO}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ZAkkaStreams {
  def recursiveSource[R, Out, State](seed: => RIO[R, State], nextState: (State, Out) => State)(
    makeSource: State => Source[Out, NotUsed]
  )(implicit env: AkkaEnv with R): Source[Out, Future[NotUsed]] = {
    val akkaEnv = env.akka
    import akkaEnv._

    Source
      .lazilyAsync(() => {
        akkaEnv.unsafeRunToFuture(seed.provide(env)).map { seedState =>
          val (actorRef, source) = Source.actorRef[State](1, OverflowStrategy.fail).preMaterialize()

          actorRef ! seedState

          source
            .flatMapConcat { state =>
              val (future, subSource) = makeSource(state)
                .viaMat(LastStateFlow[Out, State, State](state, nextState, identity))(Keep.right)
                .preMaterialize()

              subSource ++ Source.fromFutureSource(future.map {
                case (state, Success(_)) =>
                  actorRef ! state
                  Source.empty[Out]
                case (_, Failure(ex)) =>
                  actorRef ! Status.Failure(ex)
                  Source.failed[Out](ex)
              })
            }
        }
      })
      .flatMapConcat(identity)
  }

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
        val f = graph.run()(env.akka.materializer)
        f.value
          .fold(
            Task.effectAsync { cb: (Task[A] => Unit) =>
              f.onComplete {
                case Success(a) => cb(Task.succeed(a))
                case Failure(t) => cb(Task.fail(t))
              }(env.akka.dispatcher)
            }
          )(Task.fromTry(_))
      }
    }
  }

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
        val (ks, f) = graph.run()(env.akka.materializer)
        val task = f.value
          .fold(
            Task.effectAsync { cb: (Task[A] => Unit) =>
              f.onComplete {
                case Success(a) => cb(Task.succeed(a))
                case Failure(t) => cb(Task.fail(t))
              }(env.akka.dispatcher)
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
      val akkaEnv = env.akka
      import akkaEnv._

      Flow
        .lazyInitAsync(() => {
          val completionPromise = unsafeRun(zio.Promise.make[Nothing, Unit])

          Future.successful(
            Flow[A]
              .mapAsync(parallelism) { a =>
                val interruptableTask = for {
                  fib <- runTask(a).fork
                  c <- (completionPromise.await *> fib.interrupt).fork
                  ret <- fib.join
                  _ <- c.interrupt.fork
                } yield ret

                unsafeRunToFuture(interruptableTask.provide(env))
              }
              .watchTermination() { (_, f) =>
                f.onComplete { _ =>
                  val _ = unsafeRun(completionPromise.succeed(()))
                }
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
    env.akka.unsafeRun(interruptableMapAsyncM(parallelism)(runTask).provide(env))
  }

  def interruptableMapAsyncUnorderedM[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZIO.access[AkkaEnv with R] { env =>
      val akkaEnv = env.akka
      import akkaEnv._

      Flow
        .lazyInitAsync(() => {
          val completionPromise = unsafeRun(zio.Promise.make[Nothing, Unit])
          val interruption = completionPromise.await

          val flow = Flow[A]
            .mapAsyncUnordered(parallelism) { a =>
              val interruptableTask = for {
                fib <- runTask(a).fork
                c <- (interruption *> fib.interrupt).fork
                ret <- fib.join
                _ <- c.interrupt.fork
              } yield ret

              unsafeRunToFuture(interruptableTask.provide(env))
            }

          val flowWithAttrs = attributes.fold(flow)(attrs => flow.withAttributes(attrs))

          Future.successful(
            flowWithAttrs
              .watchTermination() { (_, f) =>
                f.onComplete { _ =>
                  val _ = unsafeRun(completionPromise.succeed(()))
                }
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed)(env.akka.dispatcher))
    }
  }

  def interruptableLazySource[R, A, B](effect: RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Source[B, Future[NotUsed]]] = {
    ZIO.access[AkkaEnv with R] { env =>
      val akkaEnv = env.akka
      import akkaEnv._

      Source
        .lazily(() => {
          val completionPromise = unsafeRun(zio.Promise.make[Nothing, Unit])

          val interruptableTask = for {
            fib <- effect.fork
            c <- (completionPromise.await *> fib.interrupt).fork
            ret <- fib.join
            _ <- c.interrupt.fork
          } yield ret

          Source
            .fromFuture(env.akka.unsafeRunToFuture(interruptableTask.provide(env)))
            .watchTermination() { (_, f) =>
              f.onComplete { _ =>
                val _ = unsafeRun(completionPromise.succeed(()))
              }
              NotUsed
            }
        })
    }
  }

  def switchFlatMapConcatM[R, In, Out](
    f: In => RIO[R, Source[Out, Any]]
  ): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZIO.access[AkkaEnv with R] { env =>
      val akkaEnv = env.akka
      import akkaEnv._

      Flow[In]
        .statefulMapConcat(() => {
          var currentKillSwitch = Option.empty[KillSwitch]

          in => {
            currentKillSwitch.foreach(_.shutdown())

            val (ks, s) = env.akka
              .unsafeRun(f(in).provide(env))
              .viaMat(KillSwitches.single)(Keep.right)
              .preMaterialize()

            currentKillSwitch = Some(ks)
            List(s)
          }
        })
        .async
        .flatMapConcat(identity)
    }
  }

  def switchFlatMapConcat[R, In, Out](
    f: In => RIO[R, Source[Out, Any]]
  )(implicit env: AkkaEnv with R): Flow[In, Out, NotUsed] = {
    env.akka.unsafeRun(switchFlatMapConcatM[R, In, Out](f).provide(env))
  }

  def interruptableMapAsyncUnordered[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, Future[NotUsed]] = {
    env.akka.unsafeRun(interruptableMapAsyncUnorderedM(parallelism, attributes)(runTask).provide(env))
  }

  def mapAsyncM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.access[AkkaEnv with R] { env =>
      Flow[A]
        .mapAsync(parallelism) { a =>
          env.akka.unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
        }
    }
  }

  def mapAsync[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, NotUsed] = {
    env.akka.unsafeRun(mapAsyncM(parallelism)(runTask).provide(env))
  }

  def mapAsyncUnorderedM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.access[AkkaEnv with R] { env =>
      Flow[A]
        .mapAsyncUnordered(parallelism) { a: A =>
          env.akka.unsafeRunToFuture(runTask(a).fold(Future.failed, Future.successful).provide(env)).flatten
        }
    }
  }

  def mapAsyncUnordered[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit env: AkkaEnv with R): Flow[A, B, NotUsed] = {
    env.akka.unsafeRun(mapAsyncUnorderedM(parallelism)(runTask).provide(env))
  }

  private val ecTask = Task.descriptor.map(_.executor.asEC)

  def withEc[T](make: ExecutionContext => T): Task[T] = {
    ecTask.map(make)
  }

  object ops {
    implicit class AkkaStreamFlowZioOps[-In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
      def effectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Flow[In, Next, Mat] = {
        flow
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      def effectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Flow[In, Next, Mat] = {
        flow
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def interruptableEffectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Flow[In, Next, Mat] = {
        flow
          .via(interruptableMapAsync[R, Out, Next](parallelism)(runTask))
      }

      def interruptableEffectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Flow[In, Next, Mat] = {
        flow
          .via(interruptableMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def switchFlatMapConcat[R, Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit env: AkkaEnv with R): Flow[In, Next, Mat] = {
        flow
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }

    implicit class AkkaStreamSourceZioOps[+Out, +Mat](source: Source[Out, Mat]) {
      def effectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Source[Next, Mat] = {
        source
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      def effectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Source[Next, Mat] = {
        source
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def interruptableEffectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Source[Next, Mat] = {
        source
          .via(interruptableMapAsync[R, Out, Next](parallelism)(runTask))
      }

      def interruptableEffectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit env: AkkaEnv with R): Source[Next, Mat] = {
        source
          .via(interruptableMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def switchFlatMapConcat[R, Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit env: AkkaEnv with R): Source[Next, Mat] = {
        source
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }
  }
}
