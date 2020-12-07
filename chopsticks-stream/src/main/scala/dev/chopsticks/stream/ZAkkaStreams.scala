package dev.chopsticks.stream

import akka.NotUsed
import akka.actor.Status
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Source}
import dev.chopsticks.fp.ZService
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.{IzLogging, LogCtx}
import dev.chopsticks.fp.zio_ext._
import zio.{Has, RIO, Task, UIO, ZIO}

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ZAkkaStreams {
  def recursiveSource[R <: Has[_], Out, State](seed: => RIO[R, State], nextState: (State, Out) => State)(
    makeSource: State => Source[Out, NotUsed]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Out, NotUsed] = {
    val env = rt.environment
    val akkaService = ZService.get[AkkaEnv.Service](env)
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
    make: => RunnableGraph[Future[A]]
  ): RIO[AkkaEnv with IzLogging with R, A] = {
    ZIO.accessM[AkkaEnv with IzLogging with R] { env =>
      val akkaService = ZService.get[AkkaEnv.Service](env)
      import akkaService.{actorSystem, dispatcher}

      val graph = make
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

  def graphM[R <: Has[_], A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): RIO[AkkaEnv with IzLogging with R, A] = {
    make.flatMap(graph(_))
  }

  def interruptibleGraph[A](
    make: => RunnableGraph[(KillSwitch, Future[A])],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with IzLogging, A] = {
    for {
      akkaService <- ZIO.access[AkkaEnv](_.get)
      logger <- ZIO.access[IzLogging](_.get.loggerWithCtx(ctx))
      ret <- {
        val graph = make
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
            e => logger.error(s"Graph interrupted ($graceful) which led to: ${e.getMessage -> "exception"}"),
            _ => ()
          )
        )
      }
    } yield ret
  }

  def interruptibleGraphM[R <: Has[_], A](
    make: RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with IzLogging with R, A] = {
    make.flatMap(interruptibleGraph(_, graceful))
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsync(...) instead", "2.26.2")
  def interruptibleMapAsyncM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZAkkaFlow[A].interruptibleMapAsync(parallelism)(runTask)
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsync(...) instead", "2.26.2")
  def interruptibleMapAsync[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, Future[NotUsed]] = {
    rt.unsafeRun(interruptibleMapAsyncM(parallelism)(runTask))
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsyncUnordered(...) instead", "2.26.2")
  def interruptibleMapAsyncUnorderedM[R <: Has[_], A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, Future[NotUsed]]] = {
    ZAkkaFlow[A].interruptibleMapAsyncUnordered(parallelism, attributes)(runTask)
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsyncUnordered(...) instead", "2.26.2")
  def interruptibleMapAsyncUnordered[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, Future[NotUsed]] = {
    rt.unsafeRun(interruptibleMapAsyncUnorderedM(parallelism, attributes)(runTask))
  }

  def interruptibleLazySource[R <: Has[_], A, B](
    effect: RIO[R, B]
  ): ZIO[AkkaEnv with R, Nothing, Source[B, Future[NotUsed]]] = {
    ZIO.runtime[AkkaEnv with R].map { implicit rt =>
      val env = rt.environment
      val akkaService = ZService.get[AkkaEnv.Service](env)
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

  @deprecated("Use ZAkkaFlow[In].switchFlatMapConcat(...) instead", "2.26.2")
  def switchFlatMapConcatM[R <: Has[_], In, Out](
    f: In => RIO[R, Source[Out, Any]]
  ): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZAkkaFlow[In].switchFlatMapConcat(f)
  }

  @deprecated("Use ZAkkaFlow[In].switchFlatMapConcat(...) instead", "2.26.2")
  def switchFlatMapConcat[R <: Has[_], In, Out](
    f: In => RIO[R, Source[Out, Any]]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Out, NotUsed] = {
    rt.unsafeRun(switchFlatMapConcatM[R, In, Out](f))
  }

  @deprecated("Use ZAkkaFlow[In].mapAsync(...) instead", "2.26.2")
  def mapAsyncM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZAkkaFlow[A].mapAsync(parallelism)(runTask)
  }

  @deprecated("Use ZAkkaFlow[In].mapAsync(...) instead", "2.26.2")
  def mapAsync[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(mapAsyncM(parallelism)(runTask))
  }

  @deprecated("Use ZAkkaFlow[In].mapAsyncUnordered(...) instead", "2.26.2")
  def mapAsyncUnorderedM[R <: Has[_], A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZAkkaFlow[A].mapAsyncUnordered(parallelism)(runTask)
  }

  @deprecated("Use ZAkkaFlow[In].mapAsyncUnordered(...) instead", "2.26.2")
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
      @nowarn("cat=deprecation")
      def effectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def effectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def switchFlatMapConcat[R <: Has[_], Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }

    implicit class AkkaStreamSourceZioOps[+Out, +Mat](source: Source[Out, Mat]) {
      @nowarn("cat=deprecation")
      def effectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def effectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsync[R <: Has[_], Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsyncUnordered[R <: Has[_], Next](
        parallelism: Int,
        attributes: Option[Attributes] = None
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism, attributes)(runTask))
      }

      @nowarn("cat=deprecation")
      def switchFlatMapConcat[R <: Has[_], Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }
  }
}
