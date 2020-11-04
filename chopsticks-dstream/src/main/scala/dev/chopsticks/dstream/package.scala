package dev.chopsticks

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.akka_env.AkkaEnv
import io.prometheus.client.{Counter, Gauge}
import zio.stm.{STM, TMap, TQueue}
import zio._
import zio.clock.Clock

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

package object dstream {
  type DstreamEnv[Req, Res] = Has[DstreamEnv.Service[Req, Res]]

  object DstreamEnv extends Serializable {
    final case class WorkResult[Res](source: Source[Res, NotUsed], metadata: Metadata)
    final case class InvalidAssignment[Req](assignment: Req)
        extends RuntimeException(s"Report was invoked for an invalid $assignment")

    // todo make a factory out of it instead
    trait Service[Req, Res] {
      def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]]
      def enqueueAssignment(assignment: Req): UIO[WorkResult[Res]]
      def report(assignment: Req): IO[InvalidAssignment[Req], Unit]
    }

    trait Metrics {
      def workerGauge: Gauge
      def attemptCounter: Counter
      def queueGauge: Gauge
      def mapGauge: Gauge
    }

    def any[Req, Res]: ZLayer[DstreamEnv[Req, Res], Nothing, DstreamEnv[Req, Res]] =
      ZLayer.requires[DstreamEnv[Req, Res]]

    // todo handle case when worker doesn't send full response?
    def live[Req: Tag, Res: Tag](metrics: Metrics): ZLayer[AkkaEnv with Clock, Nothing, DstreamEnv[Req, Res]] = {
      val managed =
        for {
          akkaService <- ZManaged.access[AkkaEnv](_.get)
          workerGauge = metrics.workerGauge
          attemptCounter = metrics.attemptCounter
          queueGauge = metrics.queueGauge
          mapGauge = metrics.mapGauge
          assignmentQueue <- TQueue.unbounded[Req].commit.toManaged_
          workResultMap <- TMap.empty[Req, Option[WorkResult[Res]]].commit.toManaged_
          updateQueueGauge = for {
            (queueSize, map) <- assignmentQueue.size.zip(workResultMap.toMap).commit
            _ <- UIO(queueGauge.set(queueSize.toDouble))
            _ <- UIO(mapGauge.set(map.size.toDouble))
          } yield ()
          _ <- updateQueueGauge.repeat(Schedule.fixed(500.millis.toJava)).fork.toManaged_
        } yield new DstreamEnv.Service[Req, Res] {
          import akkaService.{actorSystem, dispatcher}

          def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]] = {
            val (inFuture, inSource) = in.watchTermination() { case (_, f) => f }.preMaterialize()
            val worker = WorkResult(inSource, metadata)

            val completionSignal = Task.fromFuture(_ => inFuture).fold(_ => (), _ => ())
            val stats =
              UIO(attemptCounter.inc())
                .zipRight(UIO(workerGauge.inc()))
                .ensuring(completionSignal *> UIO(workerGauge.dec()))

            for {
              _ <- stats.forkDaemon
              assignment <- STM.atomically {
                for {
                  dequeued <- assignmentQueue.poll
                  ret <- dequeued.map(STM.succeed(_)).getOrElse(STM.retry)
                  _ <- workResultMap.put(ret, Some(worker))
                } yield ret
              }
            } yield Source.single(assignment) ++ Source.futureSource(inFuture.map(_ => Source.empty))
          }

          def enqueueAssignment(assignment: Req): UIO[WorkResult[Res]] = {
            for {
              _ <- STM.atomically {
                for {
                  isAssigned <- workResultMap.contains(assignment)
                  _ <- STM.check(!isAssigned)
                  _ <- assignmentQueue.offer(assignment)
                  _ <- workResultMap.put(assignment, None)
                } yield ()
              }
              in <- STM.atomically {
                for {
                  assignmentStatus <- workResultMap.get(assignment)
                  ret <- assignmentStatus match {
                    case Some(Some(in)) => STM.succeed(in)
                    case Some(None) => STM.retry
                    case None => STM.die(new IllegalStateException("Invalid STM state"))
                  }
                } yield ret
              }
            } yield in
          }

          def report(assignment: Req): IO[InvalidAssignment[Req], Unit] = {
            STM.atomically {
              for {
                assignmentStatus <- workResultMap.get(assignment)
                _ <- assignmentStatus match {
                  case Some(Some(_)) =>
                    workResultMap.delete(assignment)
                  case Some(None) =>
                    for {
                      _ <- workResultMap.delete(assignment)
                      queueElems <- assignmentQueue.takeAll
                      _ <- assignmentQueue.offerAll(queueElems.filterNot(_ == assignment))
                    } yield ()
                  case None =>
                    STM.fail(InvalidAssignment(assignment))
                }
              } yield ()
            }
          }
        }
      managed.toLayer
    }

  }

}
