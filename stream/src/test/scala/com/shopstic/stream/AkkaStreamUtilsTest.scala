package com.shopstic.stream

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.ImplicitSender
import com.shopstic.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class AkkaStreamUtilsTest
    extends AkkaTestKit
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  implicit val mat: ActorMaterializer = ActorMaterializer()

  "statefulMapConcatWithCompleteFlow" should {
    "emit with complete" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapConcatWithCompleteFlow(() => {
          val emitOnNext: Int => List[Int] = v => List(v)
          val emitOnComplete: () => List[Int] = () => List(11, 12, 13)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(100)
        subscriber.expectNextN(1 to 13)
        subscriber.expectComplete()
      }
    }

    "not emit complete until there's demand" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapConcatWithCompleteFlow(() => {
          val emitOnNext: Int => List[Int] = v => List(v)
          val emitOnComplete: () => List[Int] = () => List(11, 12, 13)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(10)
        subscriber.expectNextN(1 to 10)
        subscriber.expectNoMessage(1.milli)
        subscriber.requestNext(11)
        subscriber.expectNoMessage(1.milli)
        subscriber.requestNext(12)
        subscriber.expectNoMessage(1.milli)
        subscriber.requestNext(13)
        subscriber.expectComplete()
      }
    }
  }

  "statefulMapOptionWithCompleteFlow" should {
    "emit with complete" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapOptionWithCompleteFlow(() => {
          val emitOnNext: Int => Option[Int] = v => if (v % 2 == 0) Some(v) else None
          val emitOnComplete: () => Option[Int] = () => Some(12)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(100)
        subscriber.expectNextN(2 to 12 by 2)
        subscriber.expectComplete()
      }
    }

    "not emit complete until there's demand" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapOptionWithCompleteFlow(() => {
          val emitOnNext: Int => Option[Int] = v => if (v % 2 == 0) Some(v) else None
          val emitOnComplete: () => Option[Int] = () => Some(12)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(5)
        subscriber.expectNextN(Vector(2, 4, 6, 8, 10))
        subscriber.expectNoMessage(1.milli)
        subscriber.request(1)
        subscriber.expectNext(12)
        subscriber.expectComplete()
      }
    }

    "not emit complete if None" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapOptionWithCompleteFlow(() => {
          val emitOnNext: Int => Option[Int] = v => if (v % 2 == 0) Some(v) else None
          val emitOnComplete: () => Option[Int] = () => None

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(5)
        subscriber.expectNextN(Vector(2, 4, 6, 8, 10))
        subscriber.expectComplete()
      }
    }
  }

  "statefulMapWithCompleteFlow" should {
    "emit with complete" in {
      val source = Source(1 to 10)
        .via(AkkaStreamUtils.statefulMapWithCompleteFlow(() => {
          val emitOnNext: Int => Int = v => v * 2
          val emitOnComplete: () => Option[Int] = () => Some(22)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(100)
        subscriber.expectNextN(2 to 22 by 2)
        subscriber.expectComplete()
      }
    }

    "not emit complete until there's demand" in {
      val source = Source(1 to 5)
        .via(AkkaStreamUtils.statefulMapWithCompleteFlow(() => {
          val emitOnNext: Int => Int = v => v * 2
          val emitOnComplete: () => Option[Int] = () => Some(12)

          (emitOnNext, emitOnComplete)
        }))

      for (_ <- 1 to 10) {
        val subscriber = source
          .runWith(TestSink.probe[Int])

        subscriber.request(5)
        subscriber.expectNextN(Vector(2, 4, 6, 8, 10))
        subscriber.expectNoMessage(1.milli)
        subscriber.request(1)
        subscriber.expectNext(12)
        subscriber.expectComplete()
      }
    }
  }
}
