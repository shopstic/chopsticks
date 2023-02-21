/*
package dev.chopsticks.stream

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._

final class ReplayLastBroadcastHubTest
    extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown
    with TableDrivenPropertyChecks {
  "emit the last seen item for new subscriber" in {
    val (queue, broadcast) = Source
      .queue[Int](100, OverflowStrategy.fail)
      .toMat(ReplayLastBroadcastHub(1))(Keep.both)
      .run()

    val probe1 = broadcast
      .runWith(TestSink.probe)

    Await.ready(queue.offer(123), 1.second)
    probe1.requestNext() shouldBe 123

    val probe2 = broadcast
      .runWith(TestSink.probe)

    probe2.requestNext() shouldBe 123
    Await.ready(queue.offer(456), 1.second)

    probe1.requestNext() shouldBe 456
    probe2.requestNext() shouldBe 456

    val probe3 = broadcast
      .runWith(TestSink.probe)

    probe3.requestNext() shouldBe 456

    List(probe1, probe2, probe3).foreach(_.request(1))

    queue.complete()

    List(probe1, probe2, probe3).foreach(_.expectComplete())
  }
}
 */
