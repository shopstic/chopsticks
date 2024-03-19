package dev.chopsticks.dstream.test

import org.apache.pekko.NotUsed
import org.apache.pekko.grpc.GrpcClientSettings
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.stream.testkit.{TestPublisher, TestSubscriber}
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.DstreamServer.DstreamServerConfig
import dev.chopsticks.dstream.DstreamState.WorkResult
import dev.chopsticks.dstream.DstreamWorker.{DstreamWorkerConfig, DstreamWorkerRetryConfig}
import dev.chopsticks.dstream.{DstreamMaster, DstreamServer, DstreamServerHandler, DstreamWorker}
import dev.chopsticks.fp.pekko_env.PekkoEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.stream.ZAkkaSource.SourceToZAkkaSource
import eu.timepit.refined.auto._
import zio.test.TestAspect.PerTest
import zio.test._
import zio.{Cause, IO, Queue, RIO, Ref, Trace, URIO, ZIO, ZLayer}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

object DstreamTestUtils {
  implicit class ToTestZLayer[RIn, ROut](layer: ZLayer[RIn, Throwable, ROut]) {
    def forTest: ZLayer[RIn, TestFailure[Throwable], ROut] = layer.mapError(e => TestFailure.Runtime(Cause.fail(e)))
  }

  final case class FailedTestResult(result: TestResult) extends RuntimeException with NoStackTrace

  def createSourceProbe[T](): URIO[PekkoEnv, (TestPublisher.Probe[T], Source[T, NotUsed])] =
    PekkoEnv.actorSystem.map { implicit as =>
      TestSource.probe[T].preMaterialize()
    }

  def createSinkProbe[T](): URIO[PekkoEnv, (TestSubscriber.Probe[T], Sink[T, NotUsed])] =
    PekkoEnv.actorSystem.map { implicit as =>
      TestSink.probe[T].preMaterialize()
    }

  def setup[A: zio.Tag, R: zio.Tag, O: zio.Tag](
    masterConfig: DstreamMasterConfig
  ): ZLayer[DstreamWorker[A, R, O] with IzLogging with PekkoEnv with DstreamMaster[
    A,
    A,
    R,
    A
  ] with DstreamServerHandler[A, R] with DstreamServer[A, R], Throwable, DstreamTestContext[A, R]] = {
    val managed = for {
      server <- ZIO.service[DstreamServer[A, R]]
      serverBinding <- server.manage(DstreamServerConfig(port = 0, interface = "localhost"))
      master <- ZIO.service[DstreamMaster[A, A, R, A]]
      masterRequests <- Queue.unbounded[(A, WorkResult[R])]
      masterResponses <- Queue.unbounded[A]
      distributionFlow <- master.manageFlow(
        masterConfig,
        ZIO.succeed(_)
      ) {
        (assignment, result) =>
          for {
            _ <- masterRequests.offer(assignment -> result)
            ret <- masterResponses.take
          } yield ret
      }((_, task) => task)

      contextRef <- Ref.make(Option.empty[DstreamTestContext[A, R]])
      ctx <- ZIO.acquireReleaseInterruptible {
        for {
          workerRequests <- Queue.unbounded[A]
          workerResponses <- Queue.unbounded[Source[R, NotUsed]]

          masterSourceProbe <- createSourceProbe[A]()
          (masterAssignments, masterAssignmentSource) = masterSourceProbe

          logger <- IzLogging.logger

          masterSinkProbe <- createSinkProbe[A]()
          (masterOutputs, masterOutputSink) = masterSinkProbe

          masterFib <- masterAssignmentSource
            .wireTap(assignment => logger.debug(s"masterAssignmentSource >>> $assignment"))
            .async
            .toZAkkaSource
            .viaZAkkaFlow(distributionFlow)
            .viaBuilder(_.alsoTo(masterOutputSink))
            .killSwitch
            .interruptibleRunIgnore()
            .debug("master")
            .forkDaemon

          worker <- ZIO.service[DstreamWorker[A, R, O]]
          clientSettings <- PekkoEnv.actorSystem.map { implicit as =>
            GrpcClientSettings
              .connectToServiceAt("localhost", serverBinding.localAddress.getPort)
              .withTls(false)
          }
          workerFib <- worker
            .run(DstreamWorkerConfig(
              clientSettings = clientSettings,
              parallelism = 1,
              assignmentTimeout = 10.seconds
            )) { (_, assignment) =>
              for {
                _ <- workerRequests.offer(assignment)
                ret <- workerResponses.take
              } yield ret
            } { (workerId, task) =>
              task
                .forever
                .retry(DstreamWorker
                  .createRetrySchedule(
                    workerId,
                    DstreamWorkerRetryConfig(
                      retryInitialDelay = 100.millis,
                      retryBackoffFactor = 2.0,
                      retryMaxDelay = 1.second,
                      retryResetAfter = 5.seconds
                    )
                  ))
            }
            .unit
            .debug("worker")
            .forkDaemon
          result = DstreamTestContext(
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
          _ <- contextRef.set(Some(result))
        } yield result
      } {
        contextRef.get.flatMap {
          case Some(ctx) => ctx.workerFiber.interrupt *> ctx.masterFiber.interrupt
          case None => ZIO.unit
        }
      }
    } yield ctx

    ZLayer.scoped(managed)
  }

  def timeoutInterrupt(
    duration: zio.Duration
  ): TestAspectAtLeastR[Live] = {
    new PerTest.AtLeastR[Live] {
      override def perTest[R >: Nothing <: Live, E >: Nothing <: Any](test: ZIO[R, TestFailure[E], TestSuccess])(
        implicit trace: Trace
      ): ZIO[R, TestFailure[E], TestSuccess] = {
        def timeoutFailure =
          TestTimeoutException(s"Timeout of ${duration.toString} exceeded.")
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
      ZIO.unit
    }
    else {
      ZIO.fail(FailedTestResult(result))
    }
  }
}
