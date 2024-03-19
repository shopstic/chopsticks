package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.scaladsl.Metadata
import org.apache.pekko.stream.scaladsl.Sink.{fromGraph, shape}
import org.apache.pekko.stream.{ActorAttributes, Attributes, Graph, SinkShape, StreamSubscriptionTimeoutTerminationMode}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import dev.chopsticks.dstream.metric.DstreamStateMetricsManager
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.stream.FailIfEmptyFlow
import org.reactivestreams.Publisher
import zio.stm.{STM, TMap, TQueue}
import zio.{IO, Schedule, Scope, UIO, URIO, ZIO}

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.DurationConverters.ScalaDurationOps

trait DstreamState[Req, Res] {
  def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]]
  def awaitForWorker(assignmentId: DstreamState.AssignmentId): UIO[DstreamState.WorkResult[Res]]
  def enqueueAssignment(assignment: Req): UIO[DstreamState.AssignmentId]
  def report(assignmentId: DstreamState.AssignmentId)
    : IO[DstreamState.InvalidAssignment, Option[DstreamState.WorkResult[Res]]]
}

object DstreamState {
  type AssignmentId = Long

  final case class WorkResult[Res](source: Source[Res, NotUsed], metadata: Metadata, assignmentId: AssignmentId)
  final case class InvalidAssignment(assignmentId: AssignmentId)
      extends RuntimeException(s"Report was invoked for an invalid $assignmentId")

  final private case class WorkItem[Req, Res](assignment: Req, workResult: Option[WorkResult[Res]])
  private object WorkItem {
    def empty[Req, Res](assignment: Req): WorkItem[Req, Res] = WorkItem[Req, Res](assignment, Option.empty)
  }

  final private case class AssignmentItem[Req](assignmentId: AssignmentId, assignment: Req)

  private lazy val FanoutPublisherSinkCtor =
    Class.forName("org.apache.pekko.stream.impl.FanoutPublisherSink").getDeclaredConstructors.head

  private def publisherSinkWithNoSubscriptionTimeout[T]: Sink[T, Publisher[T]] = {
    fromGraph(
      FanoutPublisherSinkCtor.newInstance(
        Attributes.name("fanoutPublisherSink") and ActorAttributes.streamSubscriptionTimeout(
          Duration.Zero,
          StreamSubscriptionTimeoutTerminationMode.noop
        ),
        shape("FanoutPublisherSink")
      ).asInstanceOf[Graph[SinkShape[T], Publisher[T]]]
    )
  }

  @nowarn("cat=lint-infer-any")
  def manage[Req, Res](serviceId: String)
    : URIO[PekkoEnv with DstreamStateMetricsManager with Scope, DstreamState[Req, Res]] = {
    for {
      akkaService <- ZIO.service[PekkoEnv]
      metrics <- ZIO.serviceWithZIO[DstreamStateMetricsManager](_.manage(serviceId))
      workerGauge = metrics.workerCount
      offersCounter = metrics.offersTotal
      queueSizeGauge = metrics.queueSize
      mapSizeGauge = metrics.mapSize
      assignmentCounter = new AtomicLong(0L)
      assignmentQueue <- TQueue.unbounded[AssignmentItem[Req]].commit
      workResultMap <- TMap.empty[Long, WorkItem[Req, Res]].commit
      _ <- ZIO.acquireRelease {
        val updateGaugesTask = for {
          commitResult <- assignmentQueue.size.zip(workResultMap.size).commit
          (queueSize, mapSize) = commitResult
          _ <- ZIO.succeed {
            queueSizeGauge.set(queueSize)
            mapSizeGauge.set(mapSize)
          }
        } yield ()

        updateGaugesTask.repeat(Schedule.fixed(100.millis.toJava)).interruptible.forkDaemon
      }(_.interrupt)
    } yield new DstreamState[Req, Res] {
      import akkaService.{actorSystem, dispatcher}

      def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]] = {
        ZIO.suspendSucceed {
          val (inFuture, inPublisher) = in
            .watchTermination() { case (_, f) => f }
            .toMat(publisherSinkWithNoSubscriptionTimeout)(Keep.both)
            .run()
          val inSource = Source.fromPublisher(inPublisher)

          offersCounter.inc()
          workerGauge.inc()
          inFuture.onComplete(_ => workerGauge.dec())

          val provideAssignmentTask = for {
            assignment <- STM.atomically {
              for {
                dequeued <- assignmentQueue.poll
                a <- dequeued.map(STM.succeed(_)).getOrElse(STM.retry)
                workItem =
                  WorkItem(a.assignment, Some(WorkResult(inSource.via(FailIfEmptyFlow[Res]), metadata, a.assignmentId)))
                _ <- workResultMap.put(a.assignmentId, workItem)
              } yield a.assignment
            }
          } yield Source.single(assignment) ++ Source.futureSource(inFuture.map(_ => Source.empty))

          val workerWatchTask = ZIO
            .fromFuture(_ => inFuture)
            .ignore
            .as(Source.empty)

          provideAssignmentTask.interruptibleRace(workerWatchTask)
        }
      }

      override def awaitForWorker(assignmentId: AssignmentId): UIO[WorkResult[Res]] = {
        STM.atomically {
          for {
            maybeWorkItem <- workResultMap.get(assignmentId)
            ret <- maybeWorkItem match {
              case Some(WorkItem(_, Some(in))) => STM.succeed(in)
              case Some(WorkItem(_, None)) => STM.retry
              case None => STM.die(new IllegalStateException("Invalid STM state"))
            }
          } yield ret
        }
      }

      def enqueueAssignment(assignment: Req): UIO[AssignmentId] = {
        for {
          assignmentId <- ZIO.succeed(assignmentCounter.incrementAndGet())
          _ <- STM.atomically {
            for {
              _ <- assignmentQueue.offer(AssignmentItem(assignmentId, assignment))
              _ <- workResultMap.put(assignmentId, WorkItem.empty(assignment))
            } yield ()
          }
        } yield assignmentId
      }

      def report(assignmentId: Long): IO[InvalidAssignment, Option[WorkResult[Res]]] = {
        STM.atomically {
          for {
            maybeWorkItem <- workResultMap.get(assignmentId)
            _ <- maybeWorkItem match {
              case Some(WorkItem(_, Some(_))) =>
                workResultMap.delete(assignmentId)
              case Some(WorkItem(_, None)) =>
                for {
                  _ <- workResultMap.delete(assignmentId)
                  queueElems <- assignmentQueue.takeAll
                  _ <- assignmentQueue.offerAll(queueElems.filterNot(_.assignmentId == assignmentId))
                } yield ()
              case None =>
                STM.fail(InvalidAssignment(assignmentId))
            }
          } yield maybeWorkItem.flatMap(_.workResult)
        }
      }
    }
  }

}
