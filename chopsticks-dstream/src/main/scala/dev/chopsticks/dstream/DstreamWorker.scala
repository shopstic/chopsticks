package dev.chopsticks.dstream

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import enumeratum.EnumEntry
import enumeratum.EnumEntry.Hyphencase
import eu.timepit.refined.auto._
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import io.grpc.{Status, StatusRuntimeException}
import zio.Schedule.Decision
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
    assignmentTimeout: Timeout,
    retryInitialDelay: FiniteDuration,
    retryBackoffFactor: Double,
    retryMaxDelay: FiniteDuration,
    retryResetAfter: FiniteDuration
  )

  trait Service[Assignment, Result] {
    def run[R](config: DstreamWorkerConfig)(makeSource: Assignment => RIO[R, Source[Result, NotUsed]]): RIO[R, Unit]
  }

  val DefaultRetryPolicy: PartialFunction[Throwable, Boolean] = {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case e if e.getClass.getName.contains("akka.http.impl.engine.http2.Http2StreamHandling") => true
  }

  private[dstream] def runWorkers[Assignment: zio.Tag, Result: zio.Tag](
    config: DstreamWorkerConfig,
    makeSource: Assignment => Task[Source[Result, NotUsed]],
    retryPolicy: PartialFunction[Throwable, Boolean] = DefaultRetryPolicy
  ) = {
    ZIO.foreachPar_(1 to config.parallelism) { workerId =>
      ZManaged.accessManaged[DstreamClientMetricsManager](_.get.manage(workerId.toString))
        .use { metrics =>
          for {
            zlogger <- IzLogging.zioLogger
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

            retrySchedule = Schedule
              .identity[Throwable]
              .whileOutput(e => retryPolicy.applyOrElse(e, (_: Any) => false))

            backoffSchedule: Schedule[Any, Throwable, (Duration, Long)] = (Schedule
              .exponential(config.retryInitialDelay.toJava) || Schedule.spaced(config.retryMaxDelay.toJava))
              .resetAfter(config.retryResetAfter.toJava)

            _ <- runWorker
              .forever
              .unit
              .retry(
                (retrySchedule && backoffSchedule)
                  .onDecision {
                    case Decision.Done((exception, _)) =>
                      zlogger.error(s"$workerId will NOT retry $exception")
                    case Decision.Continue((exception, _), interval, _) =>
                      zlogger.debug(
                        s"$workerId will retry ${java.time.Duration.between(OffsetDateTime.now, interval) -> "duration"} ${exception.getMessage -> "exception"}"
                      )
                  }
              )
          } yield ()
        }
    }
  }

  def live[Assignment: zio.Tag, Result: zio.Tag](
    retryPolicy: PartialFunction[Throwable, Boolean] = DefaultRetryPolicy
  ): URLayer[
    MeasuredLogging with AkkaEnv with DstreamClient[Assignment, Result] with DstreamClientMetricsManager,
    DstreamWorker[Assignment, Result]
  ] = {
    ZRunnable(runWorkers[Assignment, Result] _)
      .toLayer[Service[Assignment, Result]] { fn =>
        new Service[Assignment, Result] {
          override def run[R](config: DstreamWorkerConfig)(makeSource: Assignment => RIO[R, Source[Result, NotUsed]])
            : RIO[R, Unit] = {
            for {
              env <- ZIO.environment[R]
              ret <- fn(config, result => makeSource(result).provide(env), retryPolicy)
            } yield ret
          }
        }
      }
  }
}
