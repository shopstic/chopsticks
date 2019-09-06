package dev.chopsticks.testkit

import java.time.{OffsetDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import zio.{UIO, ZIO}
import zio.internal.Scheduler
import zio.test.mock.MockClock
import zio.test.mock.MockClock.{FiberData, Mock}

final case class FixedMockClockService(original: Mock) extends MockClock.Service[Any] {
  def adjust(duration: zio.duration.Duration): UIO[Unit] = original.adjust(duration)

  def setTime(duration: zio.duration.Duration): UIO[Unit] = original.setTime(duration)

  def setTimeZone(zone: ZoneId): UIO[Unit] = original.setTimeZone(zone)

  def sleeps: UIO[List[zio.duration.Duration]] = original.sleeps

  def timeZone: UIO[ZoneId] = original.timeZone

  def currentTime(unit: TimeUnit): ZIO[Any, Nothing, Long] = original.currentTime(unit)

  def currentDateTime: ZIO[Any, Nothing, OffsetDateTime] = original.currentDateTime

  val nanoTime: ZIO[Any, Nothing, Long] = original.nanoTime

  def sleep(duration: zio.duration.Duration): ZIO[Any, Nothing, Unit] =
    for {
      currentNanoTime <- original.clockState.get.map(_.nanoTime)
      _ <- original.fiberState.updateSome {
        case FiberData(0) => FiberData(currentNanoTime)
      }
      ret <- original.sleep(duration)
    } yield ret

  def scheduler: ZIO[Any, Nothing, Scheduler] = original.scheduler
}
