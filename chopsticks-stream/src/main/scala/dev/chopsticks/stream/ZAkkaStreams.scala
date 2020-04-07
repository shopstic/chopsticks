package dev.chopsticks.stream

import akka.NotUsed
import akka.actor.Status
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Source}
import dev.chopsticks.fp.ZService
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.log_env.{LogCtx, LogEnv}
import dev.chopsticks.fp.zio_ext._
import zio.{Has, RIO, Task, UIO, ZIO}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ZAkkaStreams {
  def recursiveSource[R <: Has[_], Out, State](seed: => RIO[R, State], nextState: (State, Out) => State)(
    makeSource: State => Source[Out, NotUsed]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Out, NotUsed] = {
    val env = rt.environment
    val akkaService = env.get[AkkaEnv.Service]
    import akkaService.{actorSystem, dispatcher}

    Source
      .lazyFuture(() => {
        seed.map { seedState =>
          val completionMatcher: PartialFunction[Any, CompletionStrategy] = {
            case Status.Success(s: CompletionStrategy) => s
            case Status.Success(_) => CompletionStrategy.draining
          }

          val failureMatcher: PartialFunction[Any, Throwable] = {
            case Status.Failure(cause) => cause
          }

          val (actorRef, source) = Source
            .actorRef[State](
              completionMatcher,
              failureMatcher,
              1,
              OverflowStrategy.fail
            )
            .preMaterialize()

          actorRef ! seedState

          source
            .flatMapConcat { state =>
              val (future, subSource) = makeSource(state)
                .viaMat(LastStateFlow[Out, State, State](state, nextState, identity))(Keep.right)
                .preMaterialize()

              subSource ++ Source.futureSource(future.map {
                case (state, Success(_)) =>
                  actorRef ! state
                  Source.empty[Out]
                case (_, Failure(ex)) =>
                  actorRef ! Status.Failure(ex)
                  Source.failed[Out](ex)
              })
            }
        }.unsafeRunToFuture
      })
      .flatMapConcat(identity)
  }

  def graph[R <: Has[_], A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): RIO[AkkaEnv with LogEnv with R, A] = {
    graphM(make)
  }

  def graphM[R <: Has[_], A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): RIO[AkkaEnv with LogEnv with R, A] = {
    ZIO.accessM[AkkaEnv with LogEnv with R] { env =>
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService.{actorSystem, dispatcher}

      make.flatMap { graph =>
        val f = graph.run()
        f.value
          .fold {
            Task.effectAsync { cb: (Task[A] => Unit) =>
              f.onComplete {
                case Success(a) => cb(Task.succeed(a))
                case Failure(t) => cb(Task.fail(t))
              }
            }
          }(Task.fromTry(_))
      }
    }
  }

  def interruptibleGraph[R <: Has[_], A](
    make: RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with LogEnv with R, A] = {
    for {
      akkaService <- ZService[AkkaEnv.Service]
      logService <- ZService[LogEnv.Service]
      ret <- {
        make.flatMap { graph =>
          import akkaService.{actorSystem, dispatcher}
          val (ks, f) = graph.run()
          val task = f.value
            .fold {
              Task.effectAsync { cb: (Task[A] => Unit) =>
                f.onComplete {
                  case Success(a) => cb(Task.succeed(a))
                  case Failure(t) => cb(Task.fail(t))
                }
              }
            }(Task.fromTry(_))

          task.onInterrupt(
            UIO {
              if (graceful) ks.shutdown()
              else ks.abort(new InterruptedException("Stream (interruptibleGraph) was interrupted"))
            } *> task.fold(
              e =>
                logService.logger
                  .error(s"Graph interrupted (graceful=$graceful) which resulted in exception: ${e.getMessage}", e),
              _ => ()
            )
          )
        }
      }
    } yield ret
  }

  def interruptibleMapAsyncM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService._

      Flow
        .lazyFutureFlow(() => {
          val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])

          Future.successful(
            Flow[A]
              .mapAsync(parallelism) { a =>
                val interruptibleTask = for {
                  fib <- runTask(a).fork
                  c <- (completionPromise.await *> fib.interrupt).fork
                  ret <- fib.join
                  _ <- c.interrupt
                } yield ret

                interruptibleTask.unsafeRunToFuture
              }
              .watchTermination() { (_, f) =>
                f.onComplete { _ => val _ = rt.unsafeRun(completionPromise.succeed(())) }
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed))
    }
  }

  def interruptibleMapAsync[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, Future[NotUsed]] = {
    rt.unsafeRun(interruptibleMapAsyncM(parallelism)(runTask))
  }

  def interruptibleMapAsyncUnorderedM[R <: Has[_], A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService._

      Flow
        .lazyFutureFlow(() => {
          val completionPromise = rt.unsafeRun(zio.Promise.make[Nothing, Unit])
          val interruption = completionPromise.await

          val flow = Flow[A]
            .mapAsyncUnordered(parallelism) { a =>
              val interruptibleTask = for {
                fib <- runTask(a).fork
                c <- (interruption *> fib.interrupt).fork
                ret <- fib.join
                _ <- c.interrupt
              } yield ret

              interruptibleTask.unsafeRunToFuture
            }

          val flowWithAttrs = attributes.fold(flow)(attrs => flow.withAttributes(attrs))

          Future.successful(
            flowWithAttrs
              .watchTermination() { (_, f) =>
                f.onComplete { _ => val _ = rt.unsafeRun(completionPromise.succeed(())) }
                NotUsed
              }
          )
        })
        .mapMaterializedValue(_.map(_ => NotUsed)(akkaService.dispatcher))
    }
  }

  def interruptibleLazySource[R <: Has[_], A, B](
    effect: RIO[R, B]
  ): ZIO[AkkaEnv with R, Nothing, Source[B, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService._

      Source
        .lazySource(() => {
          val completionPromise = scala.concurrent.Promise[Either[Throwable, B]]()
          val task = effect.either race Task.fromFuture(_ => completionPromise.future)

          Source
            .future(task.flatMap(Task.fromEither(_)).unsafeRunToFuture)
            .watchTermination() { (_, f) =>
              f.onComplete { _ =>
                val _ =
                  completionPromise.success(Left(new InterruptedException("interruptibleLazySource was interrupted")))
              }
              NotUsed
            }
        })
    }
  }

  def switchFlatMapConcatM[R <: Has[_], In, Out](
    f: In => RIO[R, Source[Out, Any]]
  ): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { rt =>
      val env = rt.environment
      val akkaService = env.get[AkkaEnv.Service]
      import akkaService.actorSystem

      Flow[In]
        .statefulMapConcat(() => {
          var currentKillSwitch = Option.empty[KillSwitch]

          in => {
            currentKillSwitch.foreach(_.shutdown())

            val (ks, s) = rt
              .unsafeRun(f(in))
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

  def switchFlatMapConcat[R <: Has[_], In, Out](
    f: In => RIO[R, Source[Out, Any]]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Out, NotUsed] = {
    rt.unsafeRun(switchFlatMapConcatM[R, In, Out](f))
  }

  def interruptibleMapAsyncUnordered[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, Future[NotUsed]] = {
    rt.unsafeRun(interruptibleMapAsyncUnorderedM(parallelism, attributes)(runTask))
  }

  def mapAsyncM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      Flow[A]
        .mapAsync(parallelism) { a => runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten }
    }
  }

  def mapAsync[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(mapAsyncM(parallelism)(runTask))
  }

  def mapAsyncUnorderedM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      Flow[A]
        .mapAsyncUnordered(parallelism) { a: A =>
          runTask(a).fold(Future.failed, Future.successful).unsafeRunToFuture.flatten
        }
    }
  }

  def mapAsyncUnordered[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(mapAsyncUnorderedM(parallelism)(runTask))
  }

  private val ecTask = Task.descriptor.map(_.executor.asEC)

  def withEc[T](make: ExecutionContext => T): Task[T] = {
    ecTask.map(make)
  }

  object ops {
    implicit class AkkaStreamFlowZioOps[-In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
      def effectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      def effectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def interruptibleEffectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      def interruptibleEffectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def switchFlatMapConcat[R <: Has[_], Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }

    implicit class AkkaStreamSourceZioOps[+Out, +Mat](source: Source[Out, Mat]) {
      def effectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      def effectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def interruptibleEffectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      def interruptibleEffectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      def switchFlatMapConcat[R <: Has[_], Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }
  }
}
