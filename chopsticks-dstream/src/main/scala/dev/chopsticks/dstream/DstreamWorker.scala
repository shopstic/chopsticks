package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import dev.chopsticks.dstream.metric.DstreamWorkerMetricsManager
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.grpc.{Status, StatusRuntimeException}
import zio.Schedule.Decision
import zio.clock.Clock
import zio.duration.Duration
import zio.{Exit, RIO, Schedule, Task, UIO, URLayer, ZIO, ZManaged}

import java.time.OffsetDateTime
import java.util.concurrent.TimeoutException
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

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

  trait Service[Assignment, Result, Out] {
    def run[R1, R2](
      config: DstreamWorkerConfig
    )(makeSource: (WorkerId, Assignment) => RIO[R1, Source[Result, NotUsed]])(
      run: (Int, Task[Option[Assignment]]) => RIO[R2, Out]
    ): RIO[R1 with R2, List[Out]]
  }

  val defaultRetryPolicy: PartialFunction[Throwable, Boolean] = {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case e if e.getClass.getName.contains("akka.http.impl.engine.http2.Http2StreamHandling") => true
  }

  def createRetrySchedule(
    workerId: Int,
    config: DstreamWorkerRetryConfig,
    retryPolicy: PartialFunction[Throwable, Boolean] = defaultRetryPolicy
  ): Schedule[IzLogging, Throwable, Unit] = {
    val retrySchedule = Schedule
      .identity[Throwable]
      .whileOutput(e => retryPolicy.applyOrElse(e, (_: Any) => false))

    val backoffSchedule: Schedule[Any, Throwable, (Duration, Long)] = Schedule
      .exponential(config.retryInitialDelay.toJava)
      .resetAfter(config.retryResetAfter.toJava) || Schedule.spaced(config.retryMaxDelay.toJava)

    (retrySchedule && backoffSchedule)
      .onDecision {
        case Decision.Done((exception, _)) =>
          IzLogging.logger.map(_.error(s"$workerId will NOT retry $exception"))
        case Decision.Continue((exception, _), interval, _) =>
          IzLogging.logger.map(_.debug(
            s"$workerId will retry ${java.time.Duration.between(OffsetDateTime.now, interval) -> "duration"} ${exception.getMessage -> "exception"}"
          ))
      }
      .unit
  }

  private[dstream] def runWorkers[Assignment: zio.Tag, Result: zio.Tag, Out](
    config: DstreamWorkerConfig,
    makeSource: (WorkerId, Assignment) => Task[Source[Result, NotUsed]],
    run: (WorkerId, Task[Option[Assignment]]) => Task[Out]
  ): RIO[DstreamClient[Assignment, Result] with DstreamWorkerMetricsManager with IzLogging with AkkaEnv, List[Out]] = {
    for {
      taskEnv <- ZIO.environment[IzLogging with AkkaEnv]
      results <- ZIO.foreachPar((1 to config.parallelism).toList) {
        workerId =>
          ZManaged.accessManaged[DstreamWorkerMetricsManager](_.get.manage(workerId.toString))
            .use { metrics =>
              for {
                createRequest <-
                  ZIO.accessM[DstreamClient[Assignment, Result]](_.get.requestBuilder(config.clientSettings))

                runWorker = ZIO.bracketExit {
                  UIO {
                    metrics.attemptsTotal.inc()
                    metrics.workerStatus.set(1)
                  }
                } { (_, exit: Exit[Throwable, Option[Assignment]]) =>
                  UIO {
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
                    promise <- UIO(Promise[Source[Result, NotUsed]]())
                    result <- createRequest(workerId)
                      .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
                      .toZAkkaSource
                      .killSwitch
                      .viaBuilder(_.initialTimeout(config.assignmentTimeout.duration))
                      .mapAsync(1) {
                        assignment =>
                          makeSource(workerId, assignment)
                            .tap(s => UIO(promise.success(s)))
                            .zipRight(Task.fromFuture(_ => promise.future))
                            .as(assignment)
                      }
                      .interruptibleRunWith(Sink.lastOption)
                  } yield result
                }

                ret <- run(workerId, runWorker.provide(taskEnv))
              } yield ret
            }
      }
    } yield results
  }

  def live[Assignment: zio.Tag, Result: zio.Tag, Out: zio.Tag]: URLayer[
    IzLogging with AkkaEnv with Clock with DstreamClient[Assignment, Result] with DstreamWorkerMetricsManager,
    DstreamWorker[Assignment, Result, Out]
  ] = {
    ZRunnable(runWorkers[Assignment, Result, Out] _)
      .toLayer[Service[Assignment, Result, Out]] { fn =>
        new Service[Assignment, Result, Out] {
          override def run[R1, R2](
            config: DstreamWorkerConfig
          )(makeSource: (WorkerId, Assignment) => RIO[
            R1,
            Source[Result, NotUsed]
          ])(
            run: (WorkerId, Task[Option[Assignment]]) => RIO[R2, Out]
          ): RIO[R1 with R2, List[Out]] = {
            for {
              env <- ZIO.environment[R1 with R2]
              ret <- fn(
                config,
                (workerId, result) => makeSource(workerId, result).provide(env),
                (workerId, task) => {
                  run(workerId, task).provide(env)
                }
              )
            } yield ret
          }
        }
      }
  }
}
