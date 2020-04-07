package dev.chopsticks.testkit

import java.time.{OffsetDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.duration.Duration
import zio.test.environment.TestClock.{Data, FiberData, Test, WarningData}
import zio.test.environment.{Live, TestClock}
import zio._

object FixedTestClock {
  def live: ZLayer[Live, Nothing, Clock with TestClock] = {
    ZLayer.fromServiceManyManaged { (live: Live.Service) =>
      for {
        ref <- Ref.make(Data(Duration.Zero, Nil)).toManaged_
        fiberRef <- FiberRef.make(FiberData(Duration.Zero, ZoneId.of("UTC")), FiberData.combine).toManaged_
        refM <- RefM.make(WarningData.start).toManaged_
        test <- Managed.make(UIO(new FixedTestClock(Test(ref, fiberRef, live, refM)))) { _ =>
          refM.updateSome[Any, Nothing] {
            case WarningData.Start => ZIO.succeed(WarningData.done)
            case WarningData.Pending(fiber) => fiber.interrupt.as(WarningData.done)
          }
        }
      } yield Has.allOf[Clock.Service, TestClock.Service](test, test)
    }
  }
}

final case class FixedTestClock(original: TestClock.Test) extends TestClock.Service with Clock.Service {
  override def fiberTime: UIO[Duration] = original.fiberTime

  override def runAll: UIO[Unit] = original.runAll

  override def adjust(duration: zio.duration.Duration): UIO[Unit] = {
    original.adjust(duration)
  }

  override def setTime(duration: zio.duration.Duration): UIO[Unit] = original.setTime(duration)

  override def setTimeZone(zone: ZoneId): UIO[Unit] = original.setTimeZone(zone)

  override def sleeps: UIO[List[zio.duration.Duration]] = original.sleeps

  override def timeZone: UIO[ZoneId] = original.timeZone

  override def currentTime(unit: TimeUnit): ZIO[Any, Nothing, Long] = original.currentTime(unit)

  override def currentDateTime: ZIO[Any, Nothing, OffsetDateTime] = original.currentDateTime

  override val nanoTime: ZIO[Any, Nothing, Long] = original.nanoTime

  override def sleep(duration: zio.duration.Duration): ZIO[Any, Nothing, Unit] =
    for {
      currentDuration <- original.clockState.get.map(_.duration)
      _ <- original.fiberState.updateSome {
        case FiberData(duration, tz) if duration.isZero => FiberData(currentDuration, tz)
      }
      ret <- original.sleep(duration)
    } yield ret

  override def setDateTime(dateTime: OffsetDateTime): UIO[Unit] = original.setDateTime(dateTime)

  override val save: UIO[UIO[Unit]] = original.save
}
