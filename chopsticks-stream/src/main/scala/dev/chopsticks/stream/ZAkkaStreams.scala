package dev.chopsticks.stream

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, RunnableGraph, Source}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.LogCtx
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.stream.ZAkkaGraph.{InterruptibleGraphOps, UninterruptibleGraphOps}
import zio.{RIO, Task, URIO, ZIO}

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

object ZAkkaStreams {
  @deprecated("Use ZAkkaSource.recursiveSource(...) instead", "3.0.0")
  def recursiveSource[R, Out, State](seed: => RIO[R, State], nextState: (State, Out) => State)(
    makeSource: State => Source[Out, NotUsed]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Out, NotUsed] = {
    rt.unsafeRun(ZAkkaSource.recursiveSource(seed, nextState)(makeSource))
  }

  @deprecated("Use runToIO(...) extension method instead", "3.0.0")
  def graph[R, A](
    make: => RunnableGraph[Future[A]]
  ): RIO[AkkaEnv, A] = {
    make.runToIO
  }

  @deprecated("Use runToIO(...) extension method instead", "3.0.0")
  def graphM[R, A](
    make: RIO[R, RunnableGraph[Future[A]]]
  ): ZIO[AkkaEnv with R, Throwable, A] = {
    make.flatMap(graph(_))
  }

  @deprecated("Use interruptibleRun(...) extension method instead", "3.0.0")
  def interruptibleGraph[A](
    make: => RunnableGraph[(KillSwitch, Future[A])],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with MeasuredLogging, A] = {
    make.interruptibleRun(graceful)
  }

  @deprecated("Use interruptibleRun(...) extension method instead", "3.0.0")
  def interruptibleGraphM[R, A](
    make: RIO[R, RunnableGraph[(KillSwitch, Future[A])]],
    graceful: Boolean
  )(implicit ctx: LogCtx): RIO[AkkaEnv with MeasuredLogging with R, A] = {
    make.flatMap(interruptibleGraph(_, graceful))
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsync(...) instead", "2.26.2")
  def interruptibleMapAsyncM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[R with AkkaEnv, Nothing, Flow[A, B, NotUsed]] = {
    ZAkkaFlow[A].interruptibleMapAsync(parallelism)(runTask).make
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsync(...) instead", "2.26.2")
  def interruptibleMapAsync[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(interruptibleMapAsyncM(parallelism)(runTask))
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsyncUnordered(...) instead", "2.26.2")
  def interruptibleMapAsyncUnorderedM[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B]): ZAkkaFlow[R with AkkaEnv, Nothing, A, B, NotUsed] = {
    ZAkkaFlow[A].interruptibleMapAsyncUnordered(parallelism, attributes)(runTask)
  }

  @deprecated("Use ZAkkaFlow[In].interruptibleMapAsyncUnordered(...) instead", "2.26.2")
  def interruptibleMapAsyncUnordered[R, A, B](
    parallelism: Int,
    attributes: Option[Attributes] = None
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(interruptibleMapAsyncUnorderedM(parallelism, attributes)(runTask).make)
  }

  @deprecated("Use ZAkkaSource.interruptibleLazySource(...) instead", "3.0.0")
  def interruptibleLazySource[R, A, B](
    effect: RIO[R, B]
  ): URIO[AkkaEnv with R, Source[B, Future[NotUsed]]] = {
    ZAkkaSource.interruptibleLazySource(effect)
  }

  @deprecated("Use ZAkkaFlow[In].switchFlatMapConcat(...) instead", "2.26.2")
  def switchFlatMapConcatM[R, In, Out](
    f: In => RIO[R, Source[Out, Any]]
  ): ZIO[AkkaEnv with R, Nothing, Flow[In, Out, NotUsed]] = {
    ZAkkaFlow[In].switchFlatMapConcat(f).make
  }

  @deprecated("Use ZAkkaFlow[In].switchFlatMapConcat(...) instead", "2.26.2")
  def switchFlatMapConcat[R, In, Out](
    f: In => RIO[R, Source[Out, Any]]
  )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Out, NotUsed] = {
    rt.unsafeRun(switchFlatMapConcatM[R, In, Out](f))
  }

  @deprecated("Use ZAkkaFlow[In].mapAsync(...) instead", "2.26.2")
  def mapAsyncM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZAkkaFlow[A].mapAsync(parallelism)(runTask).make
  }

  @deprecated("Use ZAkkaFlow[In].mapAsync(...) instead", "2.26.2")
  def mapAsync[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[A, B, NotUsed] = {
    rt.unsafeRun(mapAsyncM(parallelism)(runTask))
  }

  @deprecated("Use ZAkkaFlow[In].mapAsyncUnordered(...) instead", "2.26.2")
  def mapAsyncUnorderedM[R, A, B](
    parallelism: Int
  )(runTask: A => RIO[R, B]): ZIO[AkkaEnv with R, Nothing, Flow[A, B, NotUsed]] = {
    ZAkkaFlow[A].mapAsyncUnordered(parallelism)(runTask).make
  }

  @deprecated("Use ZAkkaFlow[In].mapAsyncUnordered(...) instead", "2.26.2")
  def mapAsyncUnordered[R, A, B](
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
      def effectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def effectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def switchFlatMapConcat[R, Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Flow[In, Next, Mat] = {
        flow
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }

    implicit class AkkaStreamSourceZioOps[+Out, +Mat](source: Source[Out, Mat]) {
      @nowarn("cat=deprecation")
      def effectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def effectMapAsyncUnordered[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(mapAsyncUnordered[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsync[R, Next](
        parallelism: Int
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsync[R, Out, Next](parallelism)(runTask))
      }

      @nowarn("cat=deprecation")
      def interruptibleEffectMapAsyncUnordered[R, Next](
        parallelism: Int,
        attributes: Option[Attributes] = None
      )(runTask: Out => RIO[R, Next])(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(interruptibleMapAsyncUnordered[R, Out, Next](parallelism, attributes)(runTask))
      }

      @nowarn("cat=deprecation")
      def switchFlatMapConcat[R, Next](
        f: Out => RIO[R, Source[Next, Any]]
      )(implicit rt: zio.Runtime[AkkaEnv with R]): Source[Next, Mat] = {
        source
          .via(ZAkkaStreams.switchFlatMapConcat(f))
      }
    }
  }
}
