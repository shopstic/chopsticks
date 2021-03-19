package dev.chopsticks.dstream

import zio._
import zio.console._
import zio.test._
import zio.test.Assertion._
import zio.test.environment._

import HelloWorld._

object HelloWorld {
  def sayHello(id: Int): URIO[Console, Unit] =
    console.putStrLn(s"Hello, World $id!")
}

//noinspection TypeAnnotation
object HelloSpec extends DefaultRunnableSpec {
  def spec = suite("HelloWorldSpec")(
    testM("sayHello correctly displays output 1") {
      for {
        _ <- sayHello(1)
        output <- TestConsole.output
      } yield assert(output)(isNonEmpty)
    },
    testM("sayHello correctly displays output 2") {
      for {
        _ <- sayHello(2)
        output <- TestConsole.output
      } yield assert(output) {
        equalTo(Vector("Hello, World 3!\n"))
      }
    }
  )
}
