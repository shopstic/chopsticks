package dev.chopsticks.fdb.util

import java.util.UUID

import zio.clock.Clock
import zio.duration._
import zio.logging.Logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.logging.{log, LogAnnotation, LogLevel}
import zio.stream.Stream
import zio.test.Assertion._
import zio.test._
import zio.test.environment.{testEnvironment, TestEnvironment}
import zio.{Exit, Layer, Task, ZIO}

import scala.concurrent.Promise

//noinspection TypeAnnotation
object FdbIterateSourceTest extends RunnableSpec[TestEnvironment with Logging, Any] {

  override def aspects = List(TestAspect.timeoutWarning(60.seconds))

  override def runner = {
    val logFormat = "[correlation-id = %s] %s"
    val env: Layer[Nothing, Environment] = testEnvironment ++ Clock.live ++ Slf4jLogger.make { (context, message) =>
      val correlationId = LogAnnotation.CorrelationId.render(
        context.get(LogAnnotation.CorrelationId)
      )
      logFormat.format(correlationId, message)
    }
    TestRunner(TestExecutor.default(env))
  }

  def spec = suite("All tests")(
    testM("foo") {
      val eff = for {
        _ <- log("foo start")
        fooFib <- (ZIO.succeed("foo").delay(5.seconds) <* log("foo complete")).fork
        rt <- ZIO.runtime[Logging]
        _ <- Task.fromFuture { _ =>
          val promise = Promise[Unit]
          rt.unsafeRunAsync(log("to future here")) {
            case Exit.Success(value) => promise.success(value)
            case Exit.Failure(cause) => promise.failure(cause.squashTrace)
          }
          promise.future
        }
        foo <- fooFib.join
        _ <- log("foo end")
      } yield assert(foo)(equalTo("foo"))

//      log.locally(
//        LogAnnotation.Level(LogLevel.Info) andThen
//          LogAnnotation.Name("logger-name-here" :: "bleh" :: Nil) andThen
//          LogAnnotation.CorrelationId(Some(UUID.randomUUID()))
//      ) {
      eff
//      }
    },
    testM("bar") {
      for {
        _ <- log.info("bar start")
        foo <- ZIO.succeed("bar").delay(5.seconds)
        _ <- log.info("bar end")
      } yield assert(foo)(equalTo("fo"))
    }
  )
}
