package dev.chopsticks

import akka.NotUsed
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.metric.{MetricCounter, MetricGauge}
import zio.stm.{STM, TMap, TQueue}
import zio._
import zio.clock.Clock

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

package object dstream {
  type DstreamStateFactory = Has[DstreamStateFactory.Service]

  final case class WorkResult[Res](source: Source[Res, NotUsed], metadata: Metadata)
  final case class InvalidAssignment[Req](assignment: Req)
      extends RuntimeException(s"Report was invoked for an invalid $assignment")

  trait DstreamStateMetrics {
    def workerGauge: MetricGauge
    def attemptCounter: MetricCounter
    def queueGauge: MetricGauge
    def mapGauge: MetricGauge
  }

  object DstreamStateFactory {
    trait Service {
      def createStateService[Req: Tag, Res: Tag](metrics: DstreamStateMetrics): UManaged[DstreamState.Service[Req, Res]]
    }

    def live: ZLayer[AkkaEnv with Clock, Nothing, DstreamStateFactory] = {
      val managed = ZManaged.environment[AkkaEnv with Clock].map { env =>
        new Service {
          override def createStateService[Req: Tag, Res: Tag](metrics: DstreamStateMetrics) = {
            DstreamState.managed[Req, Res](metrics).provide(env)
          }
        }
      }
      managed.toLayer
    }

  }

  object DstreamState {
    trait Service[Req, Res] {
      def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]]
      def enqueueAssignment(assignment: Req): UIO[WorkResult[Res]]
      def report(assignment: Req): IO[InvalidAssignment[Req], Unit]
    }

    def managed[Req: Tag, Res: Tag](metrics: DstreamStateMetrics)
      : ZManaged[AkkaEnv with Clock, Nothing, Service[Req, Res]] = {
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
      } yield new Service[Req, Res] {
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
    }
  }

}
