package dev.chopsticks

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.scaladsl.Metadata
import akka.stream.scaladsl.Source
import io.prometheus.client.{Counter, Gauge}
import zio.ZLayer.NoDeps
import zio.stm.{STM, TRef}
import zio._

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext

package object dstream {
  type DstreamEnv[Req, Res] = Has[DstreamEnv.Service[Req, Res]]

  object DstreamEnv extends Serializable {
    final case class WorkResult[Res](source: Source[Res, NotUsed], metadata: Metadata)
    final case class InvalidAssignment[Req](assignment: Req)
        extends RuntimeException(s"Report was invoked for an invalid $assignment")

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

    abstract class LiveService[Req, Res](rt: zio.Runtime[Any], metrics: Metrics)(
      implicit as: ActorSystem,
      ec: ExecutionContext
    ) extends Service[Req, Res] {
      protected val workerGauge = metrics.workerGauge
      protected val attemptCounter = metrics.attemptCounter
      protected val queueGauge = metrics.queueGauge
      protected val mapGauge = metrics.mapGauge
      protected lazy val assignmentQueueRefBox = rt.unsafeRun(TRef.makeCommit(Queue.empty[Req]).memoize)
      protected lazy val workResultMapRefBox =
        rt.unsafeRun(TRef.makeCommit(Map.empty[Req, Option[WorkResult[Res]]]).memoize)

      protected val updateQueueGauge = for {
        assignmentQueueRef <- assignmentQueueRefBox
        workResultMapRef <- workResultMapRefBox
        state <- STM.atomically {
          assignmentQueueRef.get <*> workResultMapRef.get
        }
        (queue, map) = state
        _ <- UIO(queueGauge.set(queue.size.toDouble)) *> UIO(mapGauge.set(map.size.toDouble))
        _ <- UIO(println(s"Map: ${map
          .map {
            case (k, v) =>
              s"assignment=$k worker-id=${v.flatMap(_.metadata.getText(Dstreams.WORKER_ID_HEADER)).getOrElse("none")}"
          }
          .mkString(", ")}"))
      } yield ()

      def enqueueWorker(in: Source[Res, NotUsed], metadata: Metadata): UIO[Source[Req, NotUsed]] = {
        val (inFuture, inSource) = in.watchTermination() { case (_, f) => f }.preMaterialize()
        val worker = WorkResult(inSource, metadata)
        //      val abortTask = Task.fromFuture(_ => inFuture).fold(_ => (), _ => ())
        val abortTask = Task.never
        val stats = UIO(attemptCounter.inc()) *> UIO(workerGauge.inc())
          .ensuring(abortTask *> UIO(workerGauge.dec()))

        for {
          _ <- stats.fork
          assignmentQueueRef <- assignmentQueueRefBox
          workResultMapRef <- workResultMapRefBox
          assignmentFib <- STM.atomically {
            for {
              assignmentQueue <- assignmentQueueRef.get
              ret <- assignmentQueue.dequeueOption match {
                case Some((a, q)) =>
                  assignmentQueueRef.set(q) *> STM.succeed(a)
                case None =>
                  STM.retry
              }
              _ <- workResultMapRef.update(_.updated(ret, Some(worker)))
            } yield ret
          }.fork
          abortFib <- (abortTask *> assignmentFib.interrupt).fork
          assignment <- assignmentFib.join
          _ <- abortFib.interrupt
          //        _ <- UIO(println(s"beforeInterrupt enqueueWorker > assignment=$assignment"))
          //        _ <- abortFib.interrupt
          //        _ <- UIO(println(s"afterInterrupt enqueueWorker > assignment=$assignment"))
        } yield Source.single(assignment) ++ Source.futureSource(inFuture.map(_ => Source.empty))
      }

      def enqueueAssignment(assignment: Req): UIO[WorkResult[Res]] = {
        for {
          assignmentQueueRef <- assignmentQueueRefBox
          workResultMapRef <- workResultMapRefBox
          _ <- STM.atomically {
            for {
              assignmentResultSourceMap <- workResultMapRef.get
              _ <- STM.check(!assignmentResultSourceMap.contains(assignment))
              _ <- assignmentQueueRef.update(_.enqueue(assignment))
              _ <- workResultMapRef.update(_.updated(assignment, None))
            } yield ()
          }
          in <- STM.atomically {
            for {
              assignmentResultSourceMap <- workResultMapRef.get
              ret <- assignmentResultSourceMap.get(assignment) match {
                case Some(Some(in)) => STM.succeed(in)
                case Some(None) => STM.retry
                case None => STM.die(new IllegalStateException("Invalid STM state"))
              }
            } yield ret
          }
        } yield in
      }

      def report(assignment: Req): IO[InvalidAssignment[Req], Unit] = {
        for {
          assignmentQueueRef <- assignmentQueueRefBox
          workResultMapRef <- workResultMapRefBox
          ret <- STM.atomically {
            for {
              assignmentResultSourceMap <- workResultMapRef.get
              _ <- assignmentResultSourceMap.get(assignment) match {
                case Some(Some(_)) =>
                  workResultMapRef.update(_ - assignment)
                case Some(None) =>
                  workResultMapRef.update(_ - assignment) *> assignmentQueueRef.update(_.filterNot(_ == assignment))
                case None =>
                  STM.fail(InvalidAssignment(assignment))
              }
            } yield ()
          }
        } yield ret
      }
    }

    def any[Req, Res]: ZLayer[DstreamEnv[Req, Res], Nothing, DstreamEnv[Req, Res]] =
      ZLayer.requires[DstreamEnv[Req, Res]]

    def create[Req, Res](rt: zio.Runtime[Any], metrics: Metrics)(
      implicit as: ActorSystem,
      ec: ExecutionContext
    ): NoDeps[Nothing, Has[LiveService[Req, Res]]] = {
      ZLayer.succeed(new LiveService[Req, Res](rt, metrics) {})
    }
  }
}
