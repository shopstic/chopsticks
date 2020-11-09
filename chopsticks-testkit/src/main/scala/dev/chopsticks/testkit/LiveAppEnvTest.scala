package dev.chopsticks.testkit

import dev.chopsticks.fp.AkkaDiApp.AkkaDiAppContext
import zio.console.Console
import zio.test.Assertion.anything
import zio.test.{assertM, TestResult}
import zio.{RIO, Task}

trait LiveAppEnvTest {
  protected def bootstrapTest(
    context: AkkaDiAppContext
  ): RIO[Console, TestResult] = {
    val assertion = assertM {
      context
        .buildEnv
        .flatMap(_.build().useNow)
        .exitCode
        .flatMap { exitCode =>
          Task
            .fail(new RuntimeException(s"Test failed with non-zero exit code of: $exitCode"))
            .when(exitCode.code != 0)
        }
        .unit
    }(anything)
    assertion.ensuring(Task.fromFuture(_ => context.actorSystem.terminate()).ignore)
  }
}
