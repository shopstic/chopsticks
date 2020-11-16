package dev.chopsticks.dstream

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.stm.{STM, TMap, TQueue}
import zio._
import zio.clock.Clock

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

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

  trait Service[Req, Res] {
    def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]]
    def awaitForWorker(assignmentId: AssignmentId): UIO[WorkResult[Res]]
    def enqueueAssignment(assignment: Req): UIO[AssignmentId]
    def report(assignmentId: AssignmentId): IO[InvalidAssignment, Option[WorkResult[Res]]]
  }

  def managed[Req: Tag, Res: Tag](serviceId: String)
    : ZManaged[AkkaEnv with Clock with DstreamStateMetricsManager, Nothing, Service[Req, Res]] = {
    for {
      akkaService <- ZManaged.access[AkkaEnv](_.get)
      metrics <- ZManaged.accessManaged[DstreamStateMetricsManager](_.get.manage(serviceId))
      workerGauge = metrics.dstreamWorkerGauge
      attemptCounter = metrics.dstreamAttemptCounter
      queueGauge = metrics.dstreamQueueGauge
      mapGauge = metrics.dstreamMapGauge
      assignmentCounter = new AtomicLong(0L)
      assignmentQueue <- TQueue.unbounded[AssignmentItem[Req]].commit.toManaged_
      workResultMap <- TMap.empty[Long, WorkItem[Req, Res]].commit.toManaged_
      updateQueueGauge = for {
        (queueSize, map) <- assignmentQueue.size.zip(workResultMap.toMap).commit
        _ <- UIO(queueGauge.set(queueSize.toDouble))
        _ <- UIO(mapGauge.set(map.size.toDouble))
      } yield ()
      _ <- updateQueueGauge.repeat(Schedule.fixed(1.second.toJava)).fork.toManaged_
    } yield new Service[Req, Res] {
      import akkaService.{actorSystem, dispatcher}

      def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]] = {
        val (inFuture, inSource) = in.watchTermination() { case (_, f) => f }.preMaterialize()

        val completionSignal = Task.fromFuture(_ => inFuture).ignore
        val stats =
          UIO(attemptCounter.inc())
            .zipRight(UIO(workerGauge.inc()))
            .ensuring(completionSignal *> UIO(workerGauge.dec()))

        for {
          _ <- stats.forkDaemon
          assignment <- STM.atomically {
            for {
              dequeued <- assignmentQueue.poll
              a <- dequeued.map(STM.succeed(_)).getOrElse(STM.retry)
              workItem = WorkItem(a.assignment, Some(WorkResult(inSource, metadata, a.assignmentId)))
              _ <- workResultMap.put(a.assignmentId, workItem)
            } yield a.assignment
          }
        } yield Source.single(assignment) ++ Source.futureSource(inFuture.map(_ => Source.empty))
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
          assignmentId <- UIO(assignmentCounter.incrementAndGet())
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
