package dev.chopsticks.sample.app

import akka.stream.scaladsl.Sink
import dev.chopsticks.fp.ZAkkaApp
import dev.chopsticks.fp.ZAkkaApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource.ZStreamToZAkkaSource
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.{Chunk, ExitCode, RIO, Schedule, UIO, ZIO}

import java.time.Duration

object ZStreamTestApp extends ZAkkaApp {

  final case class RetryState(exception: Throwable, lastDuration: Duration, count: Long, elapsed: Duration)

  def run(args: List[String]): RIO[ZAkkaAppEnv, ExitCode] = {
    val retrySchedule: Schedule[Clock, Throwable, ((Duration, Long), Duration)] =
      Schedule.elapsed && Schedule.count && Schedule.exponential(100.millis)

    val action = UIO(println("Start...")) *> ZIO.fail(new IllegalStateException("Test bad")).delay(1.seconds)

    val stream = for {
      exceptionQueue <- ZStream.fromEffect(zio.Queue.unbounded[Throwable])
      retryStateQueue <- ZStream.fromEffect(zio.Queue.unbounded[((Duration, Long), Duration)])
      ret <- ZStream
        .fromEffect(
          action
            .onInterrupt(_ =>
              UIO(println("Got interrupted! Delaying...")) *> UIO(println("OK now release")).delay(3.seconds)
            )
            .retry(retrySchedule.tapInput(exceptionQueue.offer).tapOutput(retryStateQueue.offer))
            .as(Chunk[RetryState]())
        )
        .merge(
          ZStream
            .fromQueue(exceptionQueue)
            .zip(ZStream.fromQueue(retryStateQueue))
            .map { case (exception, ((elapsed, count), lastDuration)) =>
              Chunk.single(RetryState(exception, lastDuration, count, elapsed))
            }
        )
        .mapConcatChunk(identity)
    } yield ret

    val app = stream
      .toZAkkaSource()
      .killSwitch
      .interruptibleRunWith(Sink.foreach { state =>
        println(s"State: $state")
      })
      .unit

    app
      .race(ZIO.unit.delay(7.seconds))
      .orDie
      .as(ExitCode(0))
  }
}
