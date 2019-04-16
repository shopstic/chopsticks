package com.shopstic.stream

import java.time.Instant

import akka.NotUsed
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed._
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, Merge, Sink}
import akka.stream.typed.scaladsl.ActorSource
import akka.stream.{Attributes, FlowShape, Materializer, OverflowStrategy}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}

object AkkaStreamUtils {
  def balancerFlow[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = false))
      val merge = b.add(Merge[Out](workerCount))

      for (_ <- 1 to workerCount) {
        balancer ~> worker.async.addAttributes(Attributes.inputBuffer(1, 1)) ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  def uniqueFlow[Out]: Flow[Out, Out, NotUsed] = {
    statefulMapOptionFlow(() => {
      var set = Set.empty[Out]

      out => {
        if (!set.contains(out)) {
          set = set + out
          Some(out)
        }
        else None
      }
    })
  }

  def monotonicTimestampFlow[In, Out](
    seedTask: => Future[Instant]
  )(stamp: (Instant, In) => Out)(implicit ec: ExecutionContext): Flow[In, Out, Future[Option[NotUsed]]] = {
    Flow.lazyInitAsync(() => {
      seedTask.map { seed =>
        Flow[In]
          .via(statefulMapFlow(() => {
            var lastTimestamp = seed

            in => {
              val now = Instant.now
              val timestamp = if (!now.isAfter(lastTimestamp)) lastTimestamp.plusNanos(1) else now
              lastTimestamp = timestamp
              stamp(timestamp, in)
            }
          }))
      }
    })
  }

  def statefulMapConcatWithCompleteFlow[In, Out](
    funs: () => (In => immutable.Iterable[Out], () => immutable.Iterable[Out])
  ): Flow[In, Out, NotUsed] = {
    Flow.fromGraph(new StatefulMapConcatWithCompleteFlow(funs))
  }

  def statefulMapFlow[In, Out](f: () => In => Out): Flow[In, Out, NotUsed] = {
    Flow.fromGraph(new StatefulMapWithCompleteFlow(() => (f(), () => None)))
  }

  def statefulMapWithCompleteFlow[In, Out](f: () => (In => Out, () => Option[Out])): Flow[In, Out, NotUsed] = {
    Flow.fromGraph(new StatefulMapWithCompleteFlow(f))
  }

  def statefulMapOptionWithCompleteFlow[In, Out](
    funs: () => (In => Option[Out], () => Option[Out])
  ): Flow[In, Out, NotUsed] = {
    statefulMapWithCompleteFlow[In, Option[Out]](() => {
      val (a, b) = funs()
      (a, () => {
        val last = b()
        if (last.nonEmpty) Some(last) else None
      })
    }).collect { case Some(out) => out }
  }

  def statefulMapOptionFlow[In, Out](f: () => In => Option[Out]): Flow[In, Out, NotUsed] = {
    statefulMapFlow[In, Option[Out]](f).collect { case Some(out) => out }
  }

  def collectAsListSink[T]: Sink[T, Future[List[T]]] = {
    Sink.fromGraph[T, Future[List[T]]](new CollectSink[T, List[T]]())
  }

  def collectAsSetSink[T]: Sink[T, Future[Set[T]]] = {
    Sink.fromGraph[T, Future[Set[T]]](new CollectSink[T, Set[T]]())
  }

  def collectAsVectorSink[T]: Sink[T, Future[Vector[T]]] = {
    Sink.fromGraph[T, Future[Vector[T]]](new CollectSink[T, Vector[T]]())
  }

  def collectAsMapSink[K, V]: Sink[(K, V), Future[Map[K, V]]] = {
    Sink.fromGraph[(K, V), Future[Map[K, V]]](new CollectSink[(K, V), Map[K, V]]())
  }

  def distinctUntilChangedFlow[V]: Flow[V, V, NotUsed] = {
    distinctUntilChangedFlow((a, b) => a == b)
  }

  def distinctUntilChangedFlow[V](comparator: (V, V) => Boolean): Flow[V, V, NotUsed] = {
    Flow[V]
      .via(statefulMapOptionFlow(() => {
        var prior = Option.empty[V]

        next => {
          val someNext = Some(next)
          val emit = prior match {
            case Some(p) =>
              if (comparator(next, p)) None else someNext
            case None =>
              someNext
          }
          prior = someNext
          emit
        }
      }))
  }

  def batchFlow[V](
    maxBatchSize: Int,
    groupWithin: FiniteDuration
  ): Flow[V, Seq[V], NotUsed] = {
    if (groupWithin != Duration.Zero) {
      Flow[V]
        .groupedWithin(maxBatchSize, groupWithin)
    }
    else {
      Flow[V]
        .batch(maxBatchSize.toLong, { p =>
          val builder = Vector.newBuilder[V]
          builder += p
        })(_ += _)
        .map(_.result())
    }
  }

  final class HandlerLogger(name: String, logger: Logger) {
    def tag(message: String): String = {
      s"[$name] $message"
    }

    def debug(message: String): Unit = {
      if (logger.isDebugEnabled) {
        logger.debug(tag(message))
      }
    }

    def debug(template: String, arg1: Any): Unit = {
      if (logger.isDebugEnabled) {
        logger.debug(tag(template), arg1)
      }
    }

    def debug(template: String, arg1: Any, arg2: Any): Unit = {
      if (logger.isDebugEnabled) {
        logger.debug(tag(template), arg1, arg2)
      }
    }

    def debug(template: String, arg1: Any, arg2: Any, arg3: Any): Unit = {
      if (logger.isDebugEnabled) {
        logger.debug(tag(template), arg1, arg2, arg3)
      }
    }

    def debug(template: String, arg1: Any, arg2: Any, arg3: Any, arg4: Any): Unit = {
      if (logger.isDebugEnabled) {
        logger.debug(tag(template), arg1, arg2, arg3, arg4)
      }
    }

    def info(message: String): Unit = {
      logger.info(tag(message))
    }

    def warning(message: String): Unit = {
      logger.warning(tag(message))
    }

    def error(message: String): Unit = {
      logger.error(tag(message))
    }

    def error(message: String, cause: Throwable): Unit = {
      logger.error(tag(message), cause)
    }

    def fatal(message: String): Unit = {
      val tagged = tag(message)
      logger.error(tagged)
      throw new IllegalStateException(tagged)
    }
  }

  type SignalHandler[T] = PartialFunction[(ActorContext[T], Signal), Behavior[T]]

  def createHandler[T](name: String)(
    onMessage: PartialFunction[T, Behavior[T]]
  )(onSignal: PartialFunction[Signal, Behavior[T]] = PartialFunction.empty): Behavior[T] = {
    val b = Behaviors.receive[T] {
      case (ctx: ActorContext[T], message) =>
        onMessage
          .applyOrElse(message, (m: T) => {
            ctx.log.error("[{}] Unhandled message: {}", name, m)
            throw new IllegalStateException(s"[$name] Unhandled message: $m")
          })
    }

    if (onSignal != PartialFunction.empty) b.receiveSignal {
      case (_, signal) => onSignal(signal)
    }
    else b
  }

  type HandlerWithLogger[T] = HandlerLogger => PartialFunction[T, Behavior[T]]
  type SignalHandlerWithLogger[T] = HandlerLogger => PartialFunction[Signal, Behavior[T]]

  def createHandlerWithLogger[T](name: String, handler: HandlerWithLogger[T])(
    signalHandler: HandlerLogger => PartialFunction[Signal, Behavior[T]] = PartialFunction.empty
  ): Behavior[T] = {
    Behaviors.setup { ctx =>
      val logger = new HandlerLogger(name, ctx.log)
      val onMessage = handler(logger)
      val onSignal = if (signalHandler != PartialFunction.empty) signalHandler(logger) else PartialFunction.empty

      val b = Behaviors.receive[T] { (_, message) =>
        onMessage
          .applyOrElse(message, (m: T) => {
            ctx.log.error("[{}] Unhandled message: {}", name, m)
            throw new IllegalStateException(s"[$name] Unhandled message: $m")
          })
      }

      if (onSignal != PartialFunction.empty) b.receiveSignal {
        case (_, signal) => onSignal(signal)
      }
      else b
    }
  }

  def tapActorBehavior[T](actor: ActorRef[T], onMessage: (ActorContext[T], T) => Unit): Behavior[T] =
    Behaviors.setup[T] { ctx =>
      ctx.watch(actor)
      Behaviors.receive[T] { (c, m) =>
        onMessage(c, m)
        actor ! m
        Behaviors.same
      } receiveSignal {
        case (_, Terminated(`actor`)) =>
          Behaviors.stopped
      }
    }

  def proxyBehavior[T](behavior: Behavior[T]): Behavior[T] = {
    Behaviors.setup[T] { ctx =>
      val child = ctx.spawnAnonymous(behavior)
      ctx.watch(child)

      Behaviors.receive[T] { (_, m) =>
        child ! m
        Behaviors.same
      } receiveSignal {
        case (_, PreRestart | PostStop) =>
          ctx.unwatch(child)
          ctx.stop(child)
          Behaviors.stopped
        case (_, Terminated(`child`)) =>
          Behaviors.stopped
      }
    }
  }

  def throttleBehavior[T](
    behavior: Behavior[T],
    elements: Int,
    per: FiniteDuration,
    bufferSize: Int,
    terminateMessage: T
  )(
    implicit mat: Materializer
  ): Behavior[T] = Behaviors.setup[T] { ctx =>
    val child = ctx.spawnAnonymous(behavior)
    ctx.watch(child)

    val (sourceActor, sourceFuture) = ActorSource
      .actorRef[T](
        completionMatcher = PartialFunction.empty,
        failureMatcher = {
          case `terminateMessage` => new RuntimeException(s"Stream terminated due to getting $terminateMessage")
        },
        bufferSize = bufferSize,
        overflowStrategy = OverflowStrategy.fail
      )
      .throttle(elements, per)
      .toMat(Sink.foreach { v =>
        child ! v
      })(Keep.both)
      .run()

    sourceFuture.failed.foreach { _ =>
      ctx.self ! terminateMessage
    }(ctx.system.executionContext)

    Behaviors.receive[T] { (_, m) =>
      if (m == terminateMessage) {
        Behaviors.stopped
      }
      else {
        sourceActor ! m
        Behaviors.same
      }
    } receiveSignal {
      case (_, PreRestart | PostStop) =>
        ctx.unwatch(child)
        ctx.stop(child)
        sourceActor ! terminateMessage
        Behaviors.stopped
      case (_, Terminated(`child`)) =>
        Behaviors.stopped
    }
  }
}
