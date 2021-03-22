package dev.chopsticks.dstream.test

import akka.NotUsed
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.DstreamServer.DstreamServerConfig
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamWorker.{DstreamWorkerConfig, DstreamWorkerRetryConfig}
import dev.chopsticks.dstream.{DstreamMaster, DstreamServer, DstreamServerHandler, DstreamWorker}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import zio._
import zio.test.TestAspect.PerTest
import zio.test.environment._
import zio.test._

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

object DstreamTestUtils {
  implicit class ToTestZLayer[RIn, ROut](layer: ZLayer[RIn, Throwable, ROut]) {
    def forTest: ZLayer[RIn, TestFailure[Throwable], ROut] = layer.mapError(e => TestFailure.Runtime(Cause.fail(e)))
  }

  final case class FailedTestResult(result: TestResult) extends RuntimeException with NoStackTrace

  def createSourceProbe[T](): URIO[AkkaEnv, (TestPublisher.Probe[T], Source[T, NotUsed])] =
    AkkaEnv.actorSystem.map { implicit as =>
      TestSource.probe[T].preMaterialize()
    }

  def createSinkProbe[T](): URIO[AkkaEnv, (TestSubscriber.Probe[T], Sink[T, NotUsed])] =
    AkkaEnv.actorSystem.map { implicit as =>
      TestSink.probe[T].preMaterialize()
    }

  def setup[A: zio.Tag, R: zio.Tag](
    masterConfig: DstreamMasterConfig
  ): ZLayer[DstreamWorker[A, R] with MeasuredLogging with AkkaEnv with DstreamMaster[
    A,
    A,
    R,
    A
  ] with DstreamServerHandler[A, R] with DstreamServer[A, R], Throwable, Has[DstreamTestContext[A, R]]] = {
    val managed = for {
      server <- ZManaged.access[DstreamServer[A, R]](_.get)
      serverBinding <- server.manage(DstreamServerConfig(port = 0, interface = "localhost"))
      master <- ZManaged.access[DstreamMaster[A, A, R, A]](_.get)
      masterRequests <- ZQueue.unbounded[(A, WorkResult[R])].toManaged_
      masterResponses <- ZQueue.unbounded[A].toManaged_
      distributionFlow <- master.manageFlow(
        masterConfig,
        ZIO.succeed(_)
      ) {
        (assignment, result) =>
          for {
            _ <- masterRequests.offer(assignment -> result)
            ret <- masterResponses.take
          } yield ret
      } {
        Schedule.stop
      }

      ctx <- ZManaged.makeInterruptible {
        for {
          workerRequests <- ZQueue.unbounded[A]
          workerResponses <- ZQueue.unbounded[Source[R, NotUsed]]

          masterSourceProbe <- createSourceProbe[A]()
          (masterAssignments, masterAssignmentSource) = masterSourceProbe

          logger <- IzLogging.logger

          masterSinkProbe <- createSinkProbe[A]()
          (masterOutputs, masterOutputSink) = masterSinkProbe

          masterFib <- masterAssignmentSource
            .wireTap(assignment => logger.debug(s"masterAssignmentSource >>> $assignment"))
            .async
            .via(distributionFlow)
            .alsoTo(masterOutputSink)
            .toZAkkaSource
            .interruptibleRunIgnore()
            .debug("master")
            .forkDaemon

          worker <- ZIO.access[DstreamWorker[A, R]](_.get)
          clientSettings <- AkkaEnv.actorSystem.map { implicit as =>
            GrpcClientSettings
              .connectToServiceAt("localhost", serverBinding.localAddress.getPort)
              .withTls(false)
          }
          workerFib <- worker
            .run(DstreamWorkerConfig(
              clientSettings = clientSettings,
              parallelism = 1,
              assignmentTimeout = 10.seconds
            )) { assignment =>
              for {
                _ <- workerRequests.offer(assignment)
                ret <- workerResponses.take
              } yield ret
            } {
              DstreamWorker
                .createRetrySchedule(
                  _,
                  DstreamWorkerRetryConfig(
                    retryInitialDelay = 100.millis,
                    retryBackoffFactor = 2.0,
                    retryMaxDelay = 1.second,
                    retryResetAfter = 5.seconds
                  )
                )
            }
            .debug("worker")
            .forkDaemon

        } yield DstreamTestContext(
          serverBinding = serverBinding,
          masterAssignments = masterAssignments,
          masterRequests = masterRequests,
          masterResponses = masterResponses,
          masterOutputs = masterOutputs,
          workerRequests = workerRequests,
          workerResponses = workerResponses,
          masterFiber = masterFib,
          workerFiber = workerFib
        )
      } { ctx =>
        ctx.workerFiber.interrupt *> ctx.masterFiber.interrupt
      }
    } yield ctx

    managed.toLayer
  }

  def timeoutInterrupt(
    duration: zio.duration.Duration
  ): TestAspectAtLeastR[Live] = {
    import zio.duration._
    new PerTest.AtLeastR[Live] {
      def perTest[R <: Live, E](test: ZIO[R, TestFailure[E], TestSuccess]): ZIO[R, TestFailure[E], TestSuccess] = {
        def timeoutFailure =
          TestTimeoutException(s"Timeout of ${duration.render} exceeded.")
        Live
          .withLive(test)(_.either.timeout(duration).flatMap {
            case None => ZIO.fail(TestFailure.Runtime(Cause.die(timeoutFailure)))
            case Some(result) => ZIO.fromEither(result)
          })
      }
    }
  }

  def withChecks[R](body: RIO[R, TestResult]): RIO[R, TestResult] = {
    body.catchSome {
      case FailedTestResult(result) => ZIO.succeed(result)
    }
  }

  def check(result: TestResult): IO[FailedTestResult, Unit] = {
    if (result.isSuccess) {
      ZIO.succeed(())
    }
    else {
      ZIO.fail(FailedTestResult(result))
    }
  }
}
