package com.shopstic.stream

import java.time.LocalDateTime

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import com.shopstic.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.{Matchers, WordSpecLike}

class MultiMergeSortedTest
    extends AkkaTestKit
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {

  implicit val mat: ActorMaterializer = ActorMaterializer()

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
    import io.github.hamsters.{Union3, Union3Type}

    import scala.concurrent.duration._

    case class Event1(time: LocalDateTime)

    case class Event2(time: LocalDateTime)

    case class Event3(time: LocalDateTime)

    type EventType = Union3[Event1, Event2, Event3]

    def getEventTime(event: EventType): LocalDateTime = {
      if (event.v1.nonEmpty) event.v1.get.time
      else if (event.v2.nonEmpty) event.v2.get.time
      else event.v3.get.time
    }

    implicit object eventsOrdering extends Ordering[EventType] {
      override def compare(x: EventType, y: EventType): Int = {
        getEventTime(x).compareTo(getEventTime(y))
      }
    }

    val startTime = LocalDateTime.of(2016, 1, 1, 0, 0)

    val source = {
      val eventsUnion = new Union3Type[Event1, Event2, Event3]
      import eventsUnion._
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
