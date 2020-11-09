package dev.chopsticks.sample.dstreams

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import dev.chopsticks.dstream.DstreamStateMetrics.DstreamStateMetricsGroup
import dev.chopsticks.dstream.{DstreamStateFactory, DstreamStateMetricsManager}
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.fp.zio_ext.ZIOExtensions
import dev.chopsticks.metric.prom.PromMetricRegistry
import dev.chopsticks.sample.app.dstreams.{
  AdditionConfig,
  DstreamsSampleMasterApp,
  DstreamsSampleMasterAppConfig,
  DstreamsSampleWorkerApp
}
import dev.chopsticks.testkit.LiveRunnableSpec
import dev.chopsticks.testkit.LiveRunnableSpec.LiveRunnableEnv
import pureconfig.{KebabCase, PascalCase}
import zio.clock.Clock
import zio.{Task, UIO, ZIO, ZLayer, ZManaged, ZRef}
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test._

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.{Failure, Success}

object DstreamsMasterWorkerTest extends LiveRunnableSpec {

  private val masterConfig = DstreamsSampleMasterAppConfig(
    port = 0,
    partitions = 5,
    addition = AdditionConfig(
      from = 1,
      to = 100,
      iterations = 10
    ),
    expected = BigInt("295000"),
    distributionRetryInterval = 5.millis
  )

  private val typesafeConfig: Config = {
    val cfg = ConfigFactory.load().withValue("iz-logging.level", ConfigValueFactory.fromAnyRef("Crit"))
    if (!cfg.getBoolean("akka.stream.materializer.debug.fuzzing-mode")) {
      throw new IllegalArgumentException(
        "akka.stream.materializer.debug.fuzzing-mode is not 'on' for testing, config loading is not working properly?"
      )
    }
    cfg
  }

  override def spec: ZSpec[LiveRunnableEnv, Any] = {
    suite("Master worker suite")(
      testM("Master worker test") {
        val managedZio = for {
          manageServerResult <- DstreamsSampleMasterApp.manageServer
          (serverBinding, dstreamState) = manageServerResult
          port = serverBinding.localAddress.getPort
          lastValue <- ZRef.make(BigInt(0)).toManaged_
        } yield {
          import DstreamsSampleWorkerApp.runWorker
          val assertCurrentValueIsBiggerThanLastSeen =
            UIO(DstreamsSampleMasterApp.currentValue.get).flatMap { current =>
              assertM(lastValue.get)(isLessThan(current)) <* lastValue.set(current)
            }
          val assertCurrentValueIsTheSameAsLastSeen =
            UIO(DstreamsSampleMasterApp.currentValue.get).flatMap { current =>
              assertM(lastValue.get)(equalTo(current))
            }
          for {
            forkedResult <- DstreamsSampleMasterApp.calculateResult(dstreamState).log("Test: calculating result").fork

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
    ).provideSomeLayer[LiveRunnableEnv].apply(extraLayer.mapError(TestFailure.fail))
  }

  private def extraLayer = {
    val metricsManagerLayer =
      ZLayer.succeed(PromMetricRegistry.live[DstreamStateMetricsGroup]("MasterWorkerTest")) >>>
        DstreamStateMetricsManager.live
    (ZLayer.requires[Clock] ++ metricsManagerLayer ++ akkaLayer) >+>
      IzLogging.live(typesafeConfig) ++
      ZLayer.succeed(masterConfig) ++
      DstreamStateFactory.live
  }

  private def akkaLayer: ZLayer[Any, Nothing, AkkaEnv] = {
    val managed = ZManaged.make {
      UIO {
        val system = ActorSystem(
          getClass.getName
            .filter(_.isLetterOrDigit).split("\\.")
            .map(n => KebabCase.fromTokens(PascalCase.toTokens(n)))
            .mkString("-"),
          typesafeConfig
        )
        AkkaEnv.Live(system): AkkaEnv.Service
      }
    } { system =>
      Task.fromFuture(_ => system.actorSystem.terminate()).unit.orDie
    }
    managed.toLayer
  }

}
