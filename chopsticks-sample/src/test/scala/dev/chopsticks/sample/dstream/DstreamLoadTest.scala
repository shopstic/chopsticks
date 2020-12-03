package dev.chopsticks.sample.dstream

import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.sample.app.dstream.{DstreamLoadTestMasterApp, DstreamLoadTestWorkerApp}
import zio.{UIO, ZIO, ZRef}
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.TestEnvironment

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Success}

object DstreamLoadTest extends DstreamsDiRunnableSpec {

  override def spec: ZSpec[TestEnvironment, Any] = {
    suite("Load test suite")(
      diTestM("Load smoke test") {
        val managedZio = for {
          manageServerResult <- DstreamLoadTestMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          port = serverBinding.localAddress.getPort
          lastValue <- ZRef.make(BigInt(0)).toManaged_
        } yield {
          import DstreamLoadTestWorkerApp.runWorker
          val assertCurrentValueIsBiggerThanLastSeen =
            UIO(DstreamLoadTestMasterApp.currentValue.get).flatMap { current =>
              assertM(lastValue.get)(isLessThan(current)) <* lastValue.set(current)
            }
          val assertCurrentValueIsTheSameAsLastSeen =
            UIO(DstreamLoadTestMasterApp.currentValue.get).flatMap { current =>
              assertM(lastValue.get)(equalTo(current))
            }
          for {
            forkedResult <- DstreamLoadTestMasterApp.calculateResult(dstreamState).log("Test: calculating result").fork

            _ <- runWorker(port, 1, willCrash = false, willFail = false)
            _ <- assertCurrentValueIsBiggerThanLastSeen

            _ <- assertM {
              runWorker(port, 2, willCrash = true, willFail = false)
                .unit
                .either
                .map(_.fold(Failure(_), Success(_)))
            }(isFailure)
            _ <- assertCurrentValueIsTheSameAsLastSeen

            _ <- assertM {
              runWorker(port, 3, willCrash = false, willFail = true)
                .unit
                .either
                .map(_.fold(Failure(_), Success(_)))
            }(isFailure)
            _ <- assertCurrentValueIsTheSameAsLastSeen

            _ <- {
              ZIO
                .foreachPar_(1 to 4) { i => runWorker(port, 10 + i, willCrash = true, willFail = false).ignore }
                .fork
            }
            _ <- {
              ZIO
                .foreachPar_(1 to 4) { i => runWorker(port, 20 + i, willCrash = false, willFail = true).ignore }
                .fork
            }
            _ <- ZIO.foreachPar_(1 to 4) { i => runWorker(port, 30 + i, willCrash = false, willFail = false).ignore }
            _ <- assertCurrentValueIsBiggerThanLastSeen

            result <- forkedResult.join
          } yield assert(result)(equalTo(masterConfig.expected))
        }
        managedZio.use(identity)
      } @@ timeout(20.seconds.toJava)
    )
  }

}
