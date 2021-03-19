package dev.chopsticks.dstream

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.Timeout
import dev.chopsticks.fp.ZRunnable
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.MeasuredLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.grpc.{Status, StatusRuntimeException}
import zio.Schedule.Decision
import zio.duration.Duration
import zio.{RIO, Schedule, Task, UIO, URLayer, ZIO}

import java.time.OffsetDateTime
import java.util.concurrent.TimeoutException
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

object DstreamClientRunner {
  final case class DstreamClientConfig(
    parallelism: PosInt,
    assignmentTimeout: Timeout,
    retryInitialDelay: FiniteDuration,
    retryBackoffFactor: Double,
    retryMaxDelay: FiniteDuration,
    retryResetAfter: FiniteDuration
  )

  trait Service[Assignment, Result] {
    def run[R](config: DstreamClientConfig)(makeSource: Assignment => RIO[R, Source[Result, NotUsed]]): RIO[R, Unit]
  }

  val DefaultRetryPolicy: PartialFunction[Throwable, Boolean] = {
    case _: TimeoutException => true
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case e if e.getClass.getName.contains("akka.http.impl.engine.http2.Http2StreamHandling") => true
  }

  private[dstream] def runWorkers[Assignment: zio.Tag, Result: zio.Tag](
    config: DstreamClientConfig,
    makeSource: Assignment => Task[Source[Result, NotUsed]],
    retryPolicy: PartialFunction[Throwable, Boolean] = DefaultRetryPolicy
  ) = {
    ZIO.foreachPar_(1 to config.parallelism) { workerId =>
      for {
        zlogger <- ZIO.access[IzLogging](_.get.zioLogger)
        createRequest <- ZIO.accessM[DstreamClientApi[Assignment, Result]](_.get.requestBuilder)

        runWorker = for {
          promise <- UIO(Promise[Source[Result, NotUsed]]())
          result <- createRequest()
            .invoke(Source.futureSource(promise.future).mapMaterializedValue(_ => NotUsed))
            .toZAkkaSource
            .interruptible
            .viaBuilder(_.initialTimeout(config.assignmentTimeout.duration))
            .interruptibleMapAsync(1) {
              assignment =>
                makeSource(assignment)
                  .map(s => promise.success(s))
                  .zipRight(Task.fromFuture(_ => promise.future))
            }
            .interruptibleRunIgnore()
        } yield result

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
                  zlogger.warn(
                    s"$workerId will retry ${java.time.Duration.between(OffsetDateTime.now, interval) -> "duration"} ${exception.getMessage}"
                  )
              }
          )
      } yield ()
    }
  }

  def live[Assignment: zio.Tag, Result: zio.Tag](
    retryPolicy: PartialFunction[Throwable, Boolean] = DefaultRetryPolicy
  ): URLayer[AkkaEnv with MeasuredLogging with DstreamClientApi[Assignment, Result], DstreamClientRunner[
    Assignment,
    Result
  ]] = {
    ZRunnable(runWorkers[Assignment, Result] _)
      .toLayer[Service[Assignment, Result]] { fn =>
        new Service[Assignment, Result] {
          override def run[R](config: DstreamClientConfig)(makeSource: Assignment => RIO[R, Source[Result, NotUsed]])
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
