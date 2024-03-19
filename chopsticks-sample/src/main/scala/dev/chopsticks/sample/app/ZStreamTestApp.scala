package dev.chopsticks.sample.app

import org.apache.pekko.stream.scaladsl.Sink
import dev.chopsticks.fp.ZPekkoApp
import dev.chopsticks.fp.ZPekkoApp.ZAkkaAppEnv
import dev.chopsticks.stream.ZAkkaSource.ZStreamToZAkkaSource
import zio.stream.ZStream
import zio.{durationInt, Chunk, ExitCode, RIO, Schedule, ZIO}

import java.time.Duration
import scala.annotation.nowarn

object ZStreamTestApp extends ZPekkoApp {

  final case class RetryState(exception: Throwable, lastDuration: Duration, count: Long, elapsed: Duration)

  @nowarn("cat=lint-infer-any")
  def run: RIO[ZAkkaAppEnv, ExitCode] = {
    val retrySchedule =
      Schedule.elapsed && Schedule.count && Schedule.exponential(100.millis)

    val action = ZIO.succeed(println("Start...")) *> ZIO.fail(new IllegalStateException("Test bad")).delay(1.seconds)

    val stream = for {
      exceptionQueue <- ZStream.fromZIO(zio.Queue.unbounded[Throwable])
      retryStateQueue <- ZStream.fromZIO(zio.Queue.unbounded[(Duration, Long, Duration)])
      ret <- ZStream
        .fromZIO(
          action
            .onInterrupt(_ =>
              ZIO.succeed(println("Got interrupted! Delaying...")) *>
                ZIO.succeed(println("OK now release")).delay(3.seconds)
            )
            .tapError(exceptionQueue.offer(_))
            .retry(
              retrySchedule
                .tapOutput(retryStateQueue.offer)
            )
            .as(Chunk[RetryState]())
        )
        .merge(
          ZStream
            .fromQueue(exceptionQueue)
            .zip(ZStream.fromQueue(retryStateQueue))
            .map { case (exception, (elapsed, count, lastDuration)) =>
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
