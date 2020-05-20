package dev.chopsticks.sample.app

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Future
import scala.concurrent.duration._

object AkkaStreamBufferTest {
  def main(args: Array[String]): Unit = {
    implicit val as: ActorSystem = ActorSystem("foo")
    import as.dispatcher

    Source
      .repeat(())
      .withAttributes(Attributes.inputBuffer(0, 0))
      .mapAsync(1) { v =>
        println(s"MAP ASYNC HERE")
        Future.successful(akka.pattern.after(2.seconds, as.scheduler)(Future.successful(v)))
      }
      .withAttributes(Attributes.inputBuffer(0, 0))
      .mapAsync(1)(identity)
      .withAttributes(Attributes.inputBuffer(0, 0))
      .runWith(Sink.ignore)
      .onComplete(println)
  }
}
