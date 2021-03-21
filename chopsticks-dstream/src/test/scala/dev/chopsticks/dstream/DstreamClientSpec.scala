package dev.chopsticks.dstream

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.DstreamWorker.AkkaGrpcBackend
import dev.chopsticks.dstream.test.DstreamSpecEnv.SharedEnv
import dev.chopsticks.dstream.test.proto.{Assignment, Result}
import dev.chopsticks.dstream.test.{DstreamSpecEnv, DstreamTestContext, DstreamTestUtils}
import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.{Has, ZLayer}
import zio.blocking.{effectBlocking, effectBlockingInterrupt}
import zio.clock.Clock
import zio.duration._
import zio.magic._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.{testEnvironment, TestEnvironment}
import eu.timepit.refined.auto._

//noinspection TypeAnnotation
object DstreamClientSpec extends DefaultRunnableSpec with DstreamSpecEnv {
  import dev.chopsticks.dstream.test.DstreamTestUtils._

  override def runner: TestRunner[TestEnvironment, Any] = {
    TestRunner(TestExecutor.default(testEnvironment ++ Clock.live))
  }

  def createSourceProbe[T] = AkkaEnv.actorSystem.map { implicit as =>
    TestSource.probe[T]
  }

  def basicTest = withChecks {
    for {
      context <- DstreamTestContext.get[Assignment, Result]
      assignment = Assignment(1)
      _ <- effectBlocking {
        context.masterAssignments.sendNext(assignment)
        context.masterOutputs.request(1)
      }
      workerInAssignment <- context.workerRequests.take
      _ <- check(assert(workerInAssignment)(equalTo(assignment)))

      _ <- context.workerResponses.offer(Source.single(Result(2)))
      masterIn <- context.masterRequests.take
      (masterInAssignment, masterInResult) = masterIn
      _ <- check(assert(masterInAssignment)(equalTo(assignment)))

      masterInProbe <- AkkaEnv.actorSystem.map { implicit as =>
        masterInResult.source.toMat(TestSink.probe)(Keep.right).run()
      }
      _ <- effectBlockingInterrupt {
        masterInProbe.requestNext(Result(2))
        masterInProbe.expectComplete()
      }
      _ <- context.masterResponses.offer(masterInAssignment)
      masterOutputAssignment <- effectBlockingInterrupt {
        context.masterOutputs.expectNext()
      }
    } yield assert(masterOutputAssignment)(equalTo(assignment))
  }

  private lazy val nettyBackendContextLayer = DstreamTestUtils.setup[Assignment, Result](
    DstreamMasterConfig(parallelism = 1, ordered = true),
    AkkaGrpcBackend.Netty
  ).forTest

  private lazy val akkaHttpBackendContextLayer = DstreamTestUtils.setup[Assignment, Result](
    DstreamMasterConfig(parallelism = 1, ordered = true),
    AkkaGrpcBackend.AkkaHttp
  ).forTest

  private def nettyBackendLayer =
    ZLayer.fromSomeMagic[Environment with SharedEnv, Has[DstreamTestContext[Assignment, Result]]](
      promRegistryLayer,
      stateMetricRegistryFactoryLayer,
      clientMetricRegistryFactoryLayer,
      dstreamStateMetricsManagerLayer,
      dstreamClientMetricsManagerLayer,
      dstreamStateLayer,
      dstreamServerHandlerFactoryLayer,
      dstreamServerHandlerLayer,
      dstreamClientLayer,
      dstreamServerLayer,
      dstreamMasterLayer,
      dstreamWorkerLayer,
      nettyBackendContextLayer
    )

  private lazy val akkaHttpBackendLayer =
    ZLayer.fromSomeMagic[Environment with SharedEnv, Has[DstreamTestContext[Assignment, Result]]](
      promRegistryLayer,
      stateMetricRegistryFactoryLayer,
      clientMetricRegistryFactoryLayer,
      dstreamStateMetricsManagerLayer,
      dstreamClientMetricsManagerLayer,
      dstreamStateLayer,
      dstreamServerHandlerFactoryLayer,
      dstreamServerHandlerLayer,
      dstreamClientLayer,
      dstreamServerLayer,
      dstreamMasterLayer,
      dstreamWorkerLayer,
      akkaHttpBackendContextLayer
    )

  override def spec = suite("Dstream basic tests")(
    testM("should work end to end with Netty backend")(basicTest)
      .provideSomeMagicLayer[Environment with SharedEnv](nettyBackendLayer) @@ timeoutInterrupt(5.seconds),
    testM("should work end to end with Akka HTTP backend")(basicTest)
      .provideSomeMagicLayer[Environment with SharedEnv](akkaHttpBackendLayer) @@ timeoutInterrupt(5.seconds)
  )
    .provideSomeLayerShared[Environment](sharedLayer)
}
