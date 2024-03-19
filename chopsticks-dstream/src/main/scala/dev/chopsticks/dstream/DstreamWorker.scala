package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout
import dev.chopsticks.dstream.metric.DstreamWorkerMetricsManager
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.grpc.{Status, StatusRuntimeException}
import zio.Schedule.Decision
import zio.{Exit, RIO, Schedule, Task, URLayer, ZIO, ZLayer}

import java.time.OffsetDateTime
import java.util.concurrent.TimeoutException
import scala.annotation.nowarn
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

trait DstreamWorker[Assignment, Result, Out] {
  def run[R1, R2](
    config: DstreamWorker.DstreamWorkerConfig
  )(makeSource: (DstreamWorker.WorkerId, Assignment) => RIO[R1, Source[Result, NotUsed]])(
    run: (Int, Task[Option[Assignment]]) => RIO[R2, Out]
  ): RIO[R1 with R2, List[Out]]
}

object DstreamWorker {
  type WorkerId = Int

  final case class DstreamWorkerConfig(
    clientSettings: GrpcClientSettings,
    parallelism: PosInt,
    assignmentTimeout: Timeout
  )

  final case class DstreamWorkerRetryConfig(
    retryInitialDelay: FiniteDuration,
    retryBackoffFactor: Double,
    retryMaxDelay: FiniteDuration,
    retryResetAfter: FiniteDuration
  )

  val defaultRetryPolicy: PartialFunction[Throwable, Boolean] = {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case e if e.getClass.getName.contains("pekko.http.impl.engine.http2.Http2StreamHandling") => true
  }

  @nowarn("cat=lint-infer-any")
  def createRetrySchedule(
    workerId: Int,
    config: DstreamWorkerRetryConfig,
    retryPolicy: PartialFunction[Throwable, Boolean] = defaultRetryPolicy
  ): Schedule[IzLogging, Throwable, Unit] = {
    val retrySchedule = Schedule
      .identity[Throwable]
      .whileOutput(e => retryPolicy.applyOrElse(e, (_: Any) => false))

    val backoffSchedule = Schedule
      .exponential(config.retryInitialDelay.toJava)
      .resetAfter(config.retryResetAfter.toJava) || Schedule.spaced(config.retryMaxDelay.toJava)

    (retrySchedule && backoffSchedule)
      .onDecision { case (_, (exception, _), decision) =>
        decision match {
          case Decision.Done =>
            IzLogging.logger.map(_.error(s"$workerId will NOT retry $exception"))
          case Decision.Continue(interval) =>
            IzLogging.logger.map(_.debug(
              s"$workerId will retry ${java.time.Duration.between(OffsetDateTime.now, interval.end) -> "duration"} ${exception.getMessage -> "exception"}"
            ))
        }
      }
      .unit
  }

  def live[Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag]: URLayer[
    IzLogging with PekkoEnv with DstreamClient[Assignment, Result] with DstreamWorkerMetricsManager,
    DstreamWorker[Assignment, Result, Out]
  ] = {
    val effect =
      for {
        pekko <- ZIO.service[PekkoEnv]
        izLogging <- ZIO.service[IzLogging]
        metricsManager <- ZIO.service[DstreamWorkerMetricsManager]
        dstreamClient <- ZIO.service[DstreamClient[Assignment, Result]]
      } yield new DstreamWorker[Assignment, Result, Out] {
        override def run[R1, R2](config: DstreamWorkerConfig)(makeSource: (
          WorkerId,
          Assignment
        ) => RIO[R1, Source[Result, NotUsed]])(run: (WorkerId, Task[Option[Assignment]]) => RIO[R2, Out])
          : RIO[R1 with R2, List[Out]] = {
          for {
            rEnv <- ZIO.environment[R1 with R2]
            taskEnv = rEnv.add(pekko).add(izLogging)
            results <-
              ZIO.foreachPar((1 to config.parallelism).toList) {
                workerId =>
                  ZIO.scoped[R1 with R2] {
                    metricsManager
                      .manage(workerId.toString)
                      .flatMap { metrics =>
                        for {
                          createRequest <- dstreamClient.requestBuilder(config.clientSettings)
                          runWorker = ZIO.acquireReleaseExitWith {
                            ZIO.succeed {
                              metrics.attemptsTotal.inc()
                              metrics.workerStatus.set(1)
                            }
                          } { (_, exit: Exit[Throwable, Option[Assignment]]) =>
                            ZIO.succeed {
                              metrics.workerStatus.set(0)

                              exit match {
                                case Exit.Success(maybeAssignment) =>
                                  if (maybeAssignment.nonEmpty) {
                                    metrics.successesTotal.inc()
                                  }
                                  else {
                                    metrics.timeoutsTotal.inc()
                                  }

                                case Exit.Failure(cause) =>
                                  if (
                                    cause.failures.exists {
                                      case _: TimeoutException => true
                                      case _ => false
                                    }
                                  ) {
                                    metrics.timeoutsTotal.inc()
                                  }
                                  else {
                                    metrics.failuresTotal.inc()
                                  }
                              }
                            }
                          } { _ =>
                            for {
                              promise <- ZIO.succeed(Promise[Source[Result, NotUsed]]())
                              result <- createRequest(workerId)
                                .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
                                .toZAkkaSource
                                .killSwitch
                                .viaBuilder(_.initialTimeout(config.assignmentTimeout.duration))
                                .mapAsync(1) {
                                  assignment =>
                                    makeSource(workerId, assignment)
                                      .tap(s => ZIO.succeed(promise.success(s)))
                                      .zipRight(ZIO.fromFuture(_ => promise.future))
                                      .as(assignment)
                                }
                                .interruptibleRunWith(Sink.lastOption)
                            } yield result
                          }

                          ret <- run(workerId, runWorker.provideEnvironment(taskEnv))
                        } yield ret
                      }
                  }
              }
          } yield results
        }
      }: DstreamWorker[Assignment, Result, Out]

    ZLayer(effect)
  }
}
