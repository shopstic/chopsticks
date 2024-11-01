package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import dev.chopsticks.dstream.DstreamMaster.DstreamMasterConfig
import dev.chopsticks.dstream.test.DstreamSpecEnv.SharedEnv
import dev.chopsticks.dstream.test.proto.{Assignment, Result}
import dev.chopsticks.dstream.test.{DstreamSpecEnv, DstreamTestContext, DstreamTestUtils}
import dev.chopsticks.fp.pekko_env.PekkoEnv
import eu.timepit.refined.auto._
import zio.test._
import zio.{durationInt, ZIO}

//noinspection TypeAnnotation
object DstreamClientSpec extends ZIOSpecDefault with DstreamSpecEnv {
  import dev.chopsticks.dstream.test.DstreamTestUtils._

//  override def runner: TestRunner[TestEnvironment, Any] = {
//    TestRunner(TestExecutor.default(testEnvironment))
//  }

  def createSourceProbe[T] = PekkoEnv.actorSystem.map { implicit as =>
    TestSource.probe[T]
  }

  def basicTest = withChecks {
    for {
      context <- DstreamTestContext.get[Assignment, Result]
      assignment = Assignment(1)
      _ <- ZIO.attemptBlocking {
        context.masterAssignments.sendNext(assignment)
        context.masterOutputs.request(1)
      }
      workerInAssignment <- context.workerRequests.take
      _ <- check(assertTrue(workerInAssignment == assignment))

      _ <- context.workerResponses.offer(Source.single(Result(2)))
      masterIn <- context.masterRequests.take
      (masterInAssignment, masterInResult) = masterIn
      _ <- check(assertTrue(masterInAssignment == assignment))

      masterInProbe <- PekkoEnv.actorSystem.map { implicit as =>
        masterInResult.source.toMat(TestSink.probe)(Keep.right).run()
      }
      _ <- ZIO.attemptBlockingInterrupt {
        masterInProbe.requestNext(Result(2))
        masterInProbe.expectComplete()
      }
      _ <- context.masterResponses.offer(masterInAssignment)
      masterOutputAssignment <- ZIO.attemptBlockingInterrupt {
        context.masterOutputs.expectNext()
      }
    } yield assertTrue(masterOutputAssignment == assignment)
  }

  private lazy val contextLayer = DstreamTestUtils.setup[Assignment, Result, NotUsed](
    DstreamMasterConfig(serviceId = "test", parallelism = 1, ordered = true)
  ).forTest

  override def spec = suite("Dstream basic tests")(
    test("should work end to end")(basicTest) @@ timeoutInterrupt(5.seconds)
  )
    .provideSome[Environment with SharedEnv](
      promRegistryLayer,
      stateMetricRegistryFactoryLayer,
      clientMetricRegistryFactoryLayer,
      masterMetricRegistryFactoryLayer,
      dstreamStateMetricsManagerLayer,
      dstreamClientMetricsManagerLayer,
      dstreamMasterMetricsManagerLayer,
      dstreamStateLayer,
      dstreamServerHandlerFactoryLayer,
      dstreamServerHandlerLayer,
      dstreamClientLayer,
      dstreamServerLayer,
      dstreamMasterLayer,
      dstreamWorkerLayer,
      contextLayer
    )
    .provideSomeLayerShared[Environment](sharedLayer)
}
