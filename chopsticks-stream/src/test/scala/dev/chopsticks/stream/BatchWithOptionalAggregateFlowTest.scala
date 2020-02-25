package dev.chopsticks.stream

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.ImplicitSender
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}

import scala.concurrent.duration._
import AkkaStreamUtils.ops._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BatchWithOptionalAggregateFlowTest
    extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  "original Akka tests" should {
    "pass-through elements unchanged when there is no rate difference" in {
      val (source, sink) = TestSource
        .probe[Int]
        .batchWithOptionalAggregate(max = 2, seed = i => i)((a, b) => Some(a + b))
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      val sub = sink.expectSubscription()

      for (i <- 1 to 100) {
        sub.request(1)
        source.sendNext(i)
        sink.expectNext(i)
      }

      sub.cancel()
    }

    "aggregate elements while downstream is silent" in {
      val (publisher, subscriber) = TestSource
        .probe[Int]
        .batchWithOptionalAggregate(max = Long.MaxValue, seed = i => List(i))(aggregate = (ints, i) => Some(i :: ints))
        .toMat(TestSink.probe[List[Int]])(Keep.both)
        .run()
      val sub = subscriber.expectSubscription()

      for (i <- 1 to 10) {
        publisher.sendNext(i)
      }
      subscriber.expectNoMessage(1.second)
      sub.request(1)
      subscriber.expectNext(List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
      sub.cancel()
    }

    "backpressure subscriber when upstream is slower" in {
      val (publisher, subscriber) = TestSource
        .probe[Int]
        .batchWithOptionalAggregate(max = 2, seed = i => i)((a, b) => Some(a + b))
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()
      val sub = subscriber.expectSubscription()

      sub.request(1)
      publisher.sendNext(1)
      subscriber.expectNext(1)

      sub.request(1)
      subscriber.expectNoMessage(100.millis)
      publisher.sendNext(2)
      subscriber.expectNext(2)

      publisher.sendNext(3)
      publisher.sendNext(4)
      // The request can be in race with the above onNext(4) so the result would be either 3 or 7.
      subscriber.expectNoMessage(100.millis)
      sub.request(1)
      subscriber.expectNext(7)

      sub.request(1)
      subscriber.expectNoMessage(100.millis)
      sub.cancel()
    }
  }

  "new tests" should {
    "aggregate elements while downstream is silent, until aggregate returns None" in {
      val (publisher, subscriber) = TestSource
        .probe[Int]
        .batchWithOptionalAggregate(max = Long.MaxValue, seed = i => Set(i)) { (set, i) =>
          if (set.contains(i)) None
          else Some(set + i)
        }
        .toMat(TestSink.probe[Set[Int]])(Keep.both)
        .run()
      val sub = subscriber.expectSubscription()

      List(1, 2, 3, 4, 3, 4, 5, 6, 1, 6, 7, 7, 8).foreach { n => publisher.sendNext(n) }

      subscriber.expectNoMessage(100.millis)
      sub.request(1)
      subscriber.expectNext(Set(1, 2, 3, 4))
      sub.request(1)
      subscriber.expectNext(Set(3, 4, 5, 6, 1))
      sub.request(1)
      subscriber.expectNext(Set(6, 7))
      sub.request(1)
      subscriber.expectNext(Set(7, 8))
      sub.cancel()
    }
  }
}
