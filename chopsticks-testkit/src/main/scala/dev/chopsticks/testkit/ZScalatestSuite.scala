package dev.chopsticks.testkit

import org.scalatest.{Assertion, BeforeAndAfterAll, Suite}
import zio.{CancelableFuture, RIO, Task}

trait ZScalatestSuite extends BeforeAndAfterAll {
  this: Suite =>

  protected lazy val bootstrapRuntime: zio.Runtime[Any] = zio.Runtime.default

  def createRunner[R](inject: RIO[R, Assertion] => Task[Assertion]): RIO[R, Assertion] => CancelableFuture[Assertion] =
    (testCode: RIO[R, Assertion]) =>
      val effect = inject(testCode).interruptAllChildren
      zio.Unsafe.unsafe { implicit unsafe =>
        bootstrapRuntime.unsafe.runToFuture {
          effect
        }
      }
}
