package dev.chopsticks.stream

import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.ImplicitSender
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable.Queue
import scala.concurrent.duration._

final class StatefulConflateFlowTest
    extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {

  private def setup() = {
    val source = TestSource.probe[Int]
    val probe = TestSink.probe[Int]
    val flow = StatefulConflateFlow[Int, Int, Queue[Int]](
      Queue.empty,
      (s, e) => s.enqueue(e),
      s => {
        s.dequeueOption match {
          case Some((e, q)) => q -> Some(e * 10)
          case None => Queue.empty -> None
        }
      }
    )

    source
      .viaMat(flow)(Keep.left)
      .toMat(probe)(Keep.both)
      .run()
  }

  "basic" in {
    val (pub, sub) = setup()

    sub.request(1)
    sub.expectNoMessage(100.millis)
    pub.sendNext(1)
    sub.expectNext(10)

    pub.sendNext(2)
    pub.sendNext(3)
    sub.expectNoMessage(100.millis)

    sub.request(2)
    sub.expectNext(20)
    sub.expectNext(30)
  }

  "not complete when state is not empty" in {
    val (pub, sub) = setup()

    sub.request(1)
    sub.expectNoMessage(100.millis)
    pub.sendNext(1)
    sub.expectNext(10)

    pub.sendNext(2)
    pub.sendNext(3)
    pub.sendComplete()

    sub.request(1)
    sub.expectNext(20)
    sub.expectNoMessage(100.millis)

    sub.request(1)
    sub.expectNext(30)
    sub.expectNoMessage(100.millis)

    sub.request(1)
    sub.expectComplete()
  }

  "complete when state is empty" in {
    val (pub, sub) = setup()

    sub.request(1)
    sub.expectNoMessage(100.millis)
    pub.sendNext(1)
    sub.expectNext(10)

    sub.request(1)
    pub.sendComplete()
    sub.expectComplete()
  }

  "complete when downstream finishes" in {
    val (pub, sub) = setup()

    sub.request(1)
    sub.expectNoMessage(100.millis)
    pub.sendNext(1)
    sub.expectNext(10)

    pub.sendNext(2)
    pub.sendNext(3)
    sub.cancel()
    pub.expectCancellation()
  }
}
