package dev.chopsticks.fp

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, Attributes, Materializer}
import dev.chopsticks.testkit.ManualTimeAkkaTestKit.ManualClock
import dev.chopsticks.testkit.{AkkaTestKitAutoShutDown, FixedMockClockService, ManualTimeAkkaTestKit}
import org.scalatest.{Matchers, WordSpecLike}
import zio.clock.Clock
import zio.test.mock.MockClock
import zio.{UIO, ZIO}

import scala.concurrent.duration._

final class ZAkkaTest extends ManualTimeAkkaTestKit with WordSpecLike with Matchers with AkkaTestKitAutoShutDown {

  implicit val mat: Materializer = ActorMaterializer()

  "manual time" in {
    implicit object env extends AkkaEnv with MockClock {
      implicit val actorSystem: ActorSystem = system
      private val fixedMockClockService = FixedMockClockService(unsafeRun(MockClock.makeMock(MockClock.DefaultData)))
      val clock: MockClock.Service[Any] = fixedMockClockService
      val scheduler: MockClock.Service[Any] = fixedMockClockService
    }

    val clock = new ManualClock(Some(env))

    val (source, sink) = TestSource
      .probe[Int]
      .via(
        ZAkka
          .mapAsync(1) { i: Int =>
            ZIO.accessM[Clock] { env =>
              val c = env.clock
              c.sleep(zio.duration.Duration(10, TimeUnit.SECONDS)) *> UIO(i + 1)
            }
          }
          .withAttributes(Attributes.inputBuffer(initial = 0, max = 1))
      )
      .toMat(TestSink.probe[Int])(Keep.both)
      .run

    sink.request(2)
    source.sendNext(1)
    source.sendNext(2)
    sink.ensureSubscription()
    sink.expectNoMessage(100.millis)
    clock.timePasses(9.seconds)
    sink.expectNoMessage(100.millis)
    clock.timePasses(1.second)
    sink.expectNext(2)
    sink.expectNoMessage(100.millis)
    clock.timePasses(10.seconds)
    sink.expectNext(3)

    source.sendComplete()
    sink.expectComplete()
  }

}
