package dev.chopsticks.testkit

import java.time.{OffsetDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import zio.duration.Duration
import zio.{UIO, ZIO}
import zio.internal.Scheduler
import zio.test.environment.TestClock
import zio.test.environment.TestClock.FiberData

final case class FixedTestClockService(original: TestClock.Test) extends TestClock.Service[Any] {
  override def fiberTime: UIO[Duration] = original.fiberTime

  override def adjust(duration: zio.duration.Duration): UIO[Unit] = original.adjust(duration)

  override def setTime(duration: zio.duration.Duration): UIO[Unit] = original.setTime(duration)

  override def setTimeZone(zone: ZoneId): UIO[Unit] = original.setTimeZone(zone)

  override def sleeps: UIO[List[zio.duration.Duration]] = original.sleeps

  override def timeZone: UIO[ZoneId] = original.timeZone

  override def currentTime(unit: TimeUnit): ZIO[Any, Nothing, Long] = original.currentTime(unit)

  override def currentDateTime: ZIO[Any, Nothing, OffsetDateTime] = original.currentDateTime

  override val nanoTime: ZIO[Any, Nothing, Long] = original.nanoTime

  override def sleep(duration: zio.duration.Duration): ZIO[Any, Nothing, Unit] =
    for {
      currentNanoTime <- original.clockState.get.map(_.nanoTime)
      _ <- original.fiberState.updateSome {
        case FiberData(0) => FiberData(currentNanoTime)
      }
      ret <- original.sleep(duration)
    } yield ret

  override def scheduler: ZIO[Any, Nothing, Scheduler] = original.scheduler
}
