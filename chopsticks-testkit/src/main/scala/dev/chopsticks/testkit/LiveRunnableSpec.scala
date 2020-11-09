package dev.chopsticks.testkit

import zio.blocking.Blocking
import zio.test.{Annotations, RunnableSpec, Sized, TestAspect, TestExecutor, TestRunner}
import zio.test.environment.{Live, TestConsole, TestRandom, TestSystem}
import zio.{ZEnv, ZLayer}

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

object LiveRunnableSpec {
  type LiveRunnableEnv =
    ZEnv with Annotations with TestConsole with Live with TestRandom with Sized with TestSystem

  object TestEnvironment {
    val any: ZLayer[LiveRunnableEnv, Nothing, LiveRunnableEnv] =
      ZLayer.requires[LiveRunnableEnv]
    lazy val live: ZLayer[ZEnv, Nothing, LiveRunnableEnv] = {
      ZEnv.any ++
        Annotations.live ++
        Blocking.live ++
        (Live.default >>> TestConsole.debug) ++
        Live.default ++
        TestRandom.deterministic ++
        Sized.live(100) ++
        TestSystem.default
    }
  }
}

trait LiveRunnableSpec extends RunnableSpec[LiveRunnableSpec.LiveRunnableEnv, Any] {
  override val aspects: List[TestAspect[Nothing, Live, Nothing, Any]] =
    List(TestAspect.timeoutWarning(60.seconds.toJava))

  override val runner: TestRunner[LiveRunnableSpec.LiveRunnableEnv, Any] =
    TestRunner(TestExecutor.default(ZEnv.live >>> LiveRunnableSpec.TestEnvironment.live))
}
