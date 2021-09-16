package dev.chopsticks.stream

import java.time.LocalDateTime

import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

final class MultiMergeSortedTest
    extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  "sort single type" in {
    val source = MultiMergeSorted.merge[Int](
      List(
        Source(1 to 1001 by 2),
        Source(2 to 1000 by 2),
        Source(Vector(0, 1002))
      )
    )

    val probe = TestSink.probe[Int]

    val subscriber = source.runWith(probe)

    subscriber.request(1004)
    subscriber.expectNextN((0 to 1002).toVector)
    subscriber.expectComplete()
  }

  "complete when last source completes" in {
    val source = MultiMergeSorted.merge[Int](
      List(
        Source(1 to 1001 by 2),
        Source(Vector(0, 1002)),
        Source(2 to 100 by 2)
      ),
      untilLastSourceComplete = true
    )

    val probe = TestSink.probe[Int]

    val subscriber = source.runWith(probe)

    subscriber.request(1004)
    subscriber.expectNextN((0 to 100).toVector)

    // Since fuzzing mode is enabled for testing the last stream's complete signal might not
    // arrive before another element is emited
    subscriber.expectNextOrComplete() match {
      case Right(_) => subscriber.expectComplete()
      case Left(_) =>
    }
  }

  "complete when last source is empty" in {
    val source = MultiMergeSorted.merge[Int](
      List(
        Source(1 to 1001 by 2),
        Source(Vector(0, 1002)),
        Source.empty[Int]
      ),
      untilLastSourceComplete = true
    )

    val probe = TestSink.probe[Int]

    val subscriber = source.runWith(probe)

    subscriber.request(1004)
    subscriber.expectComplete()
  }

  "sort union type with custom ordering" in {
    import scala.concurrent.duration._

    sealed trait EventType

    case class Event1(time: LocalDateTime) extends EventType

    case class Event2(time: LocalDateTime) extends EventType

    case class Event3(time: LocalDateTime) extends EventType

    def getEventTime(event: EventType): LocalDateTime = {
      event match {
        case Event1(time) => time
        case Event2(time) => time
        case Event3(time) => time
      }
    }

    implicit object eventsOrdering extends Ordering[EventType] {
      override def compare(x: EventType, y: EventType): Int = {
        getEventTime(x).compareTo(getEventTime(y))
      }
    }

    val startTime = LocalDateTime.of(2016, 1, 1, 0, 0)

    val source = {
      MultiMergeSorted.merge[EventType](
        List(
          Source(1 to 1001 by 2).map(i => Event1(startTime.plusDays(i.toLong))),
          Source(2 to 1000 by 2).map(i => Event2(startTime.plusDays(i.toLong))),
          Source(Vector(0, 1002)).map(i => Event3(startTime.plusDays(i.toLong)))
        )
      )
    }

    val probe = TestSink.probe[EventType]

    val subscriber = source.runWith(probe)

    subscriber.request(1004)
    val events = subscriber.receiveWithin(1.second, 1003).map(getEventTime).toVector

    events should equal((0 to 1002).map(i => startTime.plusDays(i.toLong)).toVector)
  }
}
