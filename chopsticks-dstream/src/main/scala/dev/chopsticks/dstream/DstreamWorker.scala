package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import enumeratum.EnumEntry
import enumeratum.EnumEntry.Hyphencase
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
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
  sealed trait AkkaGrpcBackend extends EnumEntry with Hyphencase

  object AkkaGrpcBackend extends enumeratum.Enum[AkkaGrpcBackend] {
    //noinspection TypeAnnotation
    val values = findValues

    case object Netty extends AkkaGrpcBackend
    case object AkkaHttp extends AkkaGrpcBackend
  }

  final case class DstreamWorkerConfig(
    serverHost: NonEmptyString,
    serverPort: PortNumber,
    serverTls: Boolean,
    backend: AkkaGrpcBackend,
    parallelism: PosInt,
    assignmentTimeout: Timeout
  )

  final case class DstreamWorkerRetryConfig(
    retryInitialDelay: FiniteDuration,
    retryBackoffFactor: Double,
    retryMaxDelay: FiniteDuration,
    retryResetAfter: FiniteDuration
  )

  trait Service[Assignment, Result] {
    def run[R1, R2](config: DstreamWorkerConfig)(makeSource: Assignment => RIO[R1, Source[Result, NotUsed]])(
      makeRetrySchedule: Int => Schedule[R2, Throwable, Any]
    ): RIO[R1 with R2, Unit]
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

  private[dstream] def runWorkers[Assignment: zio.Tag, Result: zio.Tag](
    config: DstreamWorkerConfig,
    makeSource: Assignment => Task[Source[Result, NotUsed]],
    retryScheduleFactory: Int => Schedule[Any, Throwable, Any]
  ) = {
    ZIO.foreachPar_(1 to config.parallelism) { workerId =>
      ZManaged.accessManaged[DstreamWorkerMetricsManager](_.get.manage(workerId.toString))
        .use { metrics =>
          for {
            settings <- AkkaEnv.actorSystem.map { implicit as =>
              GrpcClientSettings
                .connectToServiceAt(config.serverHost, config.serverPort)
                .withBackend(config.backend.entryName)
                .withTls(config.serverTls)
            }
            createRequest <- ZIO.accessM[DstreamClient[Assignment, Result]](_.get.requestBuilder(settings))

            runWorker = ZIO.bracketExit {
              UIO {
                metrics.attemptsTotal.inc()
                metrics.workerStatus.set(1)
              }
            } { (_, exit: Exit[Throwable, Option[Assignment]]) =>
              UIO {
                metrics.workerStatus.set(0)

                if (exit.succeeded) {
                  if (exit.exists(_.nonEmpty)) {
                    metrics.successesTotal.inc()
                  }
                  else {
                    metrics.timeoutsTotal.inc()
                  }
                }
                else {
                  metrics.failuresTotal.inc()
                }
              }
            } { _ =>
              for {
                promise <- UIO(Promise[Source[Result, NotUsed]]())
                result <- createRequest(workerId)
                  .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
                  .toZAkkaSource
                  .interruptible
                  .viaBuilder(_.initialTimeout(config.assignmentTimeout.duration))
                  .interruptibleMapAsync(1) {
                    assignment =>
                      makeSource(assignment)
                        .tap(s => UIO(promise.success(s)))
                        .zipRight(Task.fromFuture(_ => promise.future))
                        .as(assignment)
                  }
                  .interruptibleRunWith(Sink.lastOption)
              } yield result
            }

            _ <- runWorker
              .forever
              .unit
              .retry(retryScheduleFactory(workerId))
          } yield ()
        }
    }
  }

  def live[Assignment: zio.Tag, Result: zio.Tag]: URLayer[
    IzLogging with AkkaEnv with Clock with DstreamClient[Assignment, Result] with DstreamWorkerMetricsManager,
    DstreamWorker[Assignment, Result]
  ] = {
    ZRunnable(runWorkers[Assignment, Result] _)
      .toLayer[Service[Assignment, Result]] { fn =>
        new Service[Assignment, Result] {
          override def run[R1, R2](config: DstreamWorkerConfig)(makeSource: Assignment => RIO[
            R1,
            Source[Result, NotUsed]
          ])(makeRetrySchedule: Int => Schedule[R2, Throwable, Any]): RIO[R1 with R2, Unit] = {
            for {
              env <- ZIO.environment[R1 with R2]
              ret <- fn(
                config,
                result => makeSource(result).provide(env),
                workerId => {
                  makeRetrySchedule(workerId).provide(env)
                }
              )
            } yield ret
          }
        }
      }
  }
}
