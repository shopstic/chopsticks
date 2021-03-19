package dev.chopsticks.dstream

import zio._
import zio.console._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

//noinspection TypeAnnotation
object DstreamSpec extends DefaultRunnableSpec {
  override def spec = suite("HelloWorldSpec")(
    testM("should work end to end") {
      for {
        _ <- sayHello(1)
        output <- TestConsole.output
      } yield assert(output)(isNonEmpty)
    }
  )
}
