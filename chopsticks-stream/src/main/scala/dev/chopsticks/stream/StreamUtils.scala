package dev.chopsticks.stream

import java.time.Duration

import zio.stream.*
import zio.*

object StreamUtils {

  object Pipeline:
    def mapConcat[R, E, In, Out](f: In => Iterable[Out])(implicit trace: Trace): ZPipeline[R, E, In, Out] =
      ZPipeline.map(f).flattenIterables

    def mapConcatChunk[R, E, In, Out](f: In => Chunk[Out])(implicit trace: Trace): ZPipeline[R, E, In, Out] =
      ZPipeline.map(f).flattenChunks
  end Pipeline

  extension [R, E, In, Out](pipeline: ZPipeline[R, E, In, Out])
    def toSink[R1 <: R, E1 >: E, L, Z](sink: => ZSink[R1, E1, Out, L, Z]): ZSink[R1, E1, In, L, Z] =
      ZSink.fromChannel(pipeline.toChannel.pipeToOrFail(sink.toChannel))

  //    def mapZIOPar(n: => Int)(using trace: Trace): ZPipeline[R, E, In, Chunk[Out]] =
  //      val channel = pipeline.toChannel
  //      channel.
  //      pipeline.sink
  //      ???
  //
  //  extension [R, E, A](stream: ZStream[R, E, A])
  //    def batch(maxBatchSize: Int, groupWithin: Duration): ZStream[R, E, Chunk[A]] =
  //      if (groupWithin != Duration.ZERO)
  //        stream.groupedWithin(maxBatchSize, groupWithin)
  //      else
  //        stream.aggregateAsync(ZSink.collectAllN(maxBatchSize))

  //  def balancerPipeline[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, NotUsed] = {
  //    import GraphDSL.Implicits._
  //
  //    Flow.fromGraph(GraphDSL.create() { implicit b =>
  //      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = false))
  //      val merge = b.add(Merge[Out](workerCount))
  //
  //      for (_ <- 1 to workerCount) {
  //        balancer ~> worker.async.addAttributes(Attributes.inputBuffer(1, 1)) ~> merge
  //      }
  //
  //      FlowShape(balancer.in, merge.out)
  //    })
  //  }

  //  def uniqueFlow[Out]: ZPipeline[Any, Throwable, Out, Out] = {
  //    statefulMapOptionFlow(() => {
  //      var set = Set.empty[Out]
  //
  //      out => {
  //        if (!set.contains(out)) {
  //          set = set + out
  //          Some(out)
  //        } else None
  //      }
  //    })
  //  }

  //  def monotonicTimestampFlow[In, Out](
  //    seedTask: => Future[Instant]
  //  )(stamp: (Instant, In) => Out)(implicit ec: ExecutionContext): Flow[In, Out, Future[NotUsed]] = {
  //    Flow.lazyFutureFlow(() => {
  //      seedTask.map { seed =>
  //        Flow[In]
  //          .via(statefulMapFlow(() => {
  //            var lastTimestamp = seed
  //
  //            in => {
  //              val now = Instant.now
  //              val timestamp = if (!now.isAfter(lastTimestamp)) lastTimestamp.plusNanos(1) else now
  //              lastTimestamp = timestamp
  //              stamp(timestamp, in)
  //            }
  //          }))
  //      }
  //    })
  //  }

  //  def statefulMapConcatWithCompleteFlow[In, Out](
  //    funs: () => (In => immutable.Iterable[Out], () => immutable.Iterable[Out])
  //  ): Flow[In, Out, NotUsed] = {
  //    Flow.fromGraph(new StatefulMapConcatWithCompleteFlow(funs))
  //  }

  //  def statefulMapFlow[In, Out](f: () => In => Out): ZPipeline[Any, Throwable, In, Out] =
  //    ZChannel.
  //    ZPipeline.fromChannel()
  //    Flow.fromGraph(new StatefulMapWithCompleteFlow(() => (f(), () => None)))

  //  def statefulMapWithCompleteFlow[In, Out](f: () => (In => Out, () => Option[Out])): Flow[In, Out, NotUsed] = {
  //    Flow.fromGraph(new StatefulMapWithCompleteFlow(f))
  //  }

  //  def statefulMapOptionWithCompleteFlow[In, Out](
  //    funs: () => (In => Option[Out], () => Option[Out])
  //  ): Flow[In, Out, NotUsed] = {
  //    statefulMapWithCompleteFlow[In, Option[Out]](() => {
  //      val (a, b) = funs()
  //      (
  //        a,
  //        () => {
  //          val last = b()
  //          if (last.nonEmpty) Some(last) else None
  //        }
  //      )
  //    }).collect { case Some(out) => out }
  //  }

  //  def statefulMapOptionFlow[In, Out](f: () => In => Option[Out]): ZPipeline[Any, Throwable, In, Out] = {
  //    statefulMapFlow[In, Option[Out]](f).collect { case Some(out) => out }
  //  }

  //  def distinctUntilChangedFlow[V]: Flow[V, V, NotUsed] = {
  //    distinctUntilChangedFlow((a, b) => a == b)
  //  }

  //  def distinctUntilChangedFlow[V](comparator: (V, V) => Boolean): Flow[V, V, NotUsed] = {
  //    Flow[V]
  //      .via(statefulMapOptionFlow(() => {
  //        var prior = Option.empty[V]
  //
  //        next => {
  //          val someNext = Some(next)
  //          val emit = prior match {
  //            case Some(p) =>
  //              if (comparator(next, p)) None else someNext
  //            case None =>
  //              someNext
  //          }
  //          prior = someNext
  //          emit
  //        }
  //      }))
  //  }

  def batchPipeline[V](
    maxBatchSize: Int,
    groupWithin: Duration
  ): ZPipeline[Any, Throwable, V, Chunk[V]] =
    if groupWithin != Duration.ZERO then
      ZPipeline.groupedWithin(maxBatchSize, groupWithin)
    else
      ZPipeline.aggregateAsync(ZSink.collectAllN(maxBatchSize))

  //  def batchWithOptionalAggregateFlow[In, Out](
  //    max: Long,
  //    costFn: In => Long,
  //    seed: In => Out
  //  )(aggregate: (Out, In) => Option[Out]): Flow[In, Out, NotUsed] = {
  //    Flow.fromGraph(
  //      new BatchWithOptionalAggregateFlow[In, Out](
  //        max: Long,
  //        costFn: In => Long,
  //        seed: In => Out,
  //        aggregate: (Out, In) => Option[Out]
  //      )
  //    )
  //  }

  //  type SignalHandler[T] = PartialFunction[(ActorContext[T], Signal), Behavior[T]]

  //  def createHandler[T](name: String)(
  //    onMessage: PartialFunction[T, Behavior[T]]
  //  )(onSignal: PartialFunction[Signal, Behavior[T]] = PartialFunction.empty): Behavior[T] = {
  //    val b = Behaviors.receive[T] {
  //      case (ctx: ActorContext[T], message) =>
  //        onMessage
  //          .applyOrElse(
  //            message,
  //            (m: T) => {
  //              ctx.log.error("[{}] Unhandled message: {}", name, m)
  //              throw new IllegalStateException(s"[$name] Unhandled message: $m")
  //            }
  //          )
  //    }
  //
  //    if (onSignal != PartialFunction.empty) b.receiveSignal {
  //      case (_, signal) => onSignal(signal)
  //    }
  //    else b
  //  }

  //  type HandlerWithLogger[T] = Logger => PartialFunction[T, Behavior[T]]
  //  type SignalHandlerWithLogger[T] = Logger => PartialFunction[Signal, Behavior[T]]

  //  def createHandlerWithLogger[T](name: String, handler: HandlerWithLogger[T])(
  //    signalHandler: Logger => PartialFunction[Signal, Behavior[T]] = PartialFunction.empty
  //  ): Behavior[T] = {
  //    Behaviors.setup { ctx =>
  //      val logger = ctx.log
  //      val onMessage = handler(logger)
  //      val onSignal = if (signalHandler != PartialFunction.empty) signalHandler(logger) else PartialFunction.empty
  //
  //      val b = Behaviors.receive[T] { (_, message) =>
  //        onMessage
  //          .applyOrElse(
  //            message,
  //            (m: T) => {
  //              ctx.log.error("[{}] Unhandled message: {}", name, m)
  //              throw new IllegalStateException(s"[$name] Unhandled message: $m")
  //            }
  //          )
  //      }
  //
  //      if (onSignal != PartialFunction.empty) b.receiveSignal {
  //        case (_, signal) => onSignal(signal)
  //      }
  //      else b
  //    }
  //  }

  //  def tapActorBehavior[T](actor: ActorRef[T], onMessage: (ActorContext[T], T) => Unit): Behavior[T] =
  //    Behaviors.setup[T] { ctx =>
  //      ctx.watch(actor)
  //      Behaviors.receive[T] { (c, m) =>
  //        onMessage(c, m)
  //        actor ! m
  //        Behaviors.same
  //      } receiveSignal {
  //        case (_, Terminated(`actor`)) =>
  //          Behaviors.stopped
  //      }
  //    }
  //
  //  def proxyBehavior[T](behavior: Behavior[T]): Behavior[T] = {
  //    Behaviors.setup[T] { ctx =>
  //      val child = ctx.spawnAnonymous(behavior)
  //      ctx.watch(child)
  //
  //      Behaviors.receive[T] { (_, m) =>
  //        child ! m
  //        Behaviors.same
  //      } receiveSignal {
  //        case (_, PreRestart | PostStop) =>
  //          ctx.unwatch(child)
  //          ctx.stop(child)
  //          Behaviors.stopped
  //        case (_, Terminated(`child`)) =>
  //          Behaviors.stopped
  //      }
  //    }
  //  }
  //
  //  def throttleBehavior[T](
  //    behavior: Behavior[T],
  //    elements: Int,
  //    per: FiniteDuration,
  //    bufferSize: Int,
  //    terminateMessage: T
  //  )(implicit
  //    mat: Materializer
  //  ): Behavior[T] = Behaviors.setup[T] { ctx =>
  //    val child = ctx.spawnAnonymous(behavior)
  //    ctx.watch(child)
  //
  //    val (sourceActor, sourceFuture) = ActorSource
  //      .actorRef[T](
  //        completionMatcher = PartialFunction.empty,
  //        failureMatcher = {
  //          case `terminateMessage` => new RuntimeException(s"Stream terminated due to getting $terminateMessage")
  //        },
  //        bufferSize = bufferSize,
  //        overflowStrategy = OverflowStrategy.fail
  //      )
  //      .throttle(elements, per)
  //      .toMat(Sink.foreach { v => child ! v })(Keep.both)
  //      .run()
  //
  //    sourceFuture.failed.foreach { _ => ctx.self ! terminateMessage }(ctx.system.executionContext)
  //
  //    Behaviors.receive[T] { (_, m) =>
  //      if (m == terminateMessage) {
  //        Behaviors.stopped
  //      } else {
  //        sourceActor ! m
  //        Behaviors.same
  //      }
  //    } receiveSignal {
  //      case (_, PreRestart | PostStop) =>
  //        ctx.unwatch(child)
  //        ctx.stop(child)
  //        sourceActor ! terminateMessage
  //        Behaviors.stopped
  //      case (_, Terminated(`child`)) =>
  //        Behaviors.stopped
  //    }
  //  }
  //
  //  object ops {
  //    implicit class AkkaStreamUtilsFlowOps[-In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
  //      def batchWithOptionalAggregate[Next](max: Long, seed: Out => Next)(
  //        aggregate: (Next, Out) => Option[Next]
  //      ): Flow[In, Next, Mat] = {
  //        flow.via(
  //          batchWithOptionalAggregateFlow[Out, Next](max, _ => 1, seed)(aggregate)
  //        )
  //      }
  //
  //      def batchWeightedWithOptionalAggregate[Next](max: Long, costFn: Out => Long, seed: Out => Next)(
  //        aggregate: (Next, Out) => Option[Next]
  //      ): Flow[In, Next, Mat] = {
  //        flow.via(
  //          batchWithOptionalAggregateFlow[Out, Next](max, costFn, seed)(aggregate)
  //        )
  //      }
  //    }
  //
  //    implicit class AkkaStreamUtilsSourceOps[+Out, +Mat](source: Source[Out, Mat]) {
  //      def batchWithOptionalAggregate[Next](max: Long, seed: Out => Next)(
  //        aggregate: (Next, Out) => Option[Next]
  //      ): Source[Next, Mat] = {
  //        source.via(
  //          batchWithOptionalAggregateFlow[Out, Next](max, _ => 1, seed)(aggregate)
  //        )
  //      }
  //
  //      def batchWeightedWithOptionalAggregate[Next](max: Long, costFn: Out => Long, seed: Out => Next)(
  //        aggregate: (Next, Out) => Option[Next]
  //      ): Source[Next, Mat] = {
  //        source.via(
  //          batchWithOptionalAggregateFlow[Out, Next](max, costFn, seed)(aggregate)
  //        )
  //      }
  //    }
  //  }
}
