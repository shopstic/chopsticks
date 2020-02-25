package dev.chopsticks.testkit

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import dev.chopsticks.testkit.ManualTimeAkkaTestKit.ManualClock
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._

final class ManualTimeAkkaTestKitTest
    extends ManualTimeAkkaTestKit
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  "plain Akka stream" in {
    import system.dispatcher
    val clock = new ManualClock()

    val (source, sink) = TestSource
      .probe[Int]
      .mapAsync(1) { i => akka.pattern.after(10.seconds, system.scheduler)(Future.successful(i + 1)) }
      .toMat(TestSink.probe[Int])(Keep.both)
      .run

    sink.request(1)
    source.sendNext(1)
    sink.expectNoMessage(100.millis)
    clock.timePasses(9.seconds)
    sink.expectNoMessage(100.millis)
    clock.timePasses(1.second)
    sink.expectNext(2)
    source.sendComplete()
    sink.expectComplete()
  }
}
