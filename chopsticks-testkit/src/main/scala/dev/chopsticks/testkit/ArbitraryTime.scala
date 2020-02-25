package dev.chopsticks.testkit

import java.time._

import org.scalacheck.{Arbitrary, Gen}
import scala.jdk.CollectionConverters._

object ArbitraryTime {
  lazy val uniformMonth: Gen[Month] = Gen.oneOf(Month.values().toIndexedSeq)

  lazy val uniformDayOfWeek: Gen[DayOfWeek] = Gen.oneOf(DayOfWeek.values().toIndexedSeq)

  lazy val uniformPeriod: Gen[Period] = Gen.choose(Int.MinValue, Int.MaxValue) map { d => Period.ofDays(d) }

  lazy val uniformDuration: Gen[Duration] = Gen.choose(Long.MinValue, Long.MaxValue) map { n => Duration.ofNanos(n) }

  lazy val uniformLocalDateTime: Gen[LocalDateTime] = uniformDuration map { n: Duration =>
    LocalDateTime.of(1970, Month.JANUARY, 1, 0, 0, 0, 0).plus(n)
  }

  lazy val uniformLocalDate: Gen[LocalDate] = uniformPeriod map { d => LocalDate.of(1970, Month.JANUARY, 1).plus(d) }

  lazy val uniformLocalTime: Gen[LocalTime] = Gen.choose(0L, 24L * 60L * 60L - 1L) map { n =>
    LocalTime.ofSecondOfDay(n)
  }

  lazy val uniformInstant: Gen[Instant] = for {
    epochSecond <- Gen.choose(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
    nanoAdjustment <- Gen.choose(Instant.MIN.getNano, Instant.MAX.getNano)
  } yield Instant.ofEpochSecond(epochSecond, nanoAdjustment.toLong)

  lazy val uniformMonthDay: Gen[MonthDay] = for {
    month <- uniformMonth
    _ <- Gen.oneOf(true, false)
    dayOfMonth <- Gen.choose(month.minLength(), month.maxLength())
  } yield MonthDay.of(month, dayOfMonth)

  lazy val uniformZoneId: Gen[ZoneId] = Gen.oneOf(ZoneId.getAvailableZoneIds.asScala.toSeq) map { zid =>
    ZoneId.of(zid)
  }

  lazy val uniformZoneOffset: Gen[ZoneOffset] = for {
    hours <- Gen.choose(-18, 18)
    //The mins and secs must have the same sign as hours and not go over 18 hours.
    maxTicks = 59 * (if (hours >= 0) 1 else -1) * (if (math.abs(hours) >= 18) 0 else 1)
    minutes <- Gen.choose(0, maxTicks)
    seconds <- Gen.choose(0, maxTicks)
  } yield ZoneOffset.ofHoursMinutesSeconds(hours, minutes, seconds)

  lazy val uniformOffsetDateTime: Gen[OffsetDateTime] = for {
    localDateTime <- uniformLocalDateTime
    zoneOffset <- uniformZoneOffset
  } yield OffsetDateTime.of(localDateTime, zoneOffset)

  lazy val uniformOffsetTime: Gen[OffsetTime] = for {
    localTime <- uniformLocalTime
    zoneOffset <- uniformZoneOffset
  } yield OffsetTime.of(localTime, zoneOffset)

  lazy val uniformYear: Gen[Year] = Gen.choose(Year.MIN_VALUE, Year.MAX_VALUE) map { y => Year.of(y) }

  lazy val uniformYearMonth: Gen[YearMonth] = for {
    year <- uniformYear
    month <- uniformMonth
  } yield YearMonth.of(year.getValue, month)

  lazy val uniformZonedDateTime: Gen[ZonedDateTime] = for {
    localDateTime <- uniformLocalDateTime
    zoneId <- uniformZoneId
  } yield ZonedDateTime.of(localDateTime, zoneId)

  lazy val maxZonedDateTimes: Gen[ZonedDateTime] = uniformZoneId map { zid => ZonedDateTime.of(LocalDateTime.MAX, zid) }

  lazy val minZonedDateTimes: Gen[ZonedDateTime] = uniformZoneId map { zid => ZonedDateTime.of(LocalDateTime.MIN, zid) }

  implicit lazy val arbitraryMonth: Arbitrary[Month] = Arbitrary(uniformMonth)

  implicit lazy val arbitraryDuration: Arbitrary[Duration] = Arbitrary(
    Gen.frequency(
      (1, Duration.ZERO),
      (10, uniformDuration)
    )
  )

  implicit lazy val arbitraryPeriod: Arbitrary[Period] = Arbitrary(
    Gen.frequency(
      (1, Period.ZERO),
      (10, uniformPeriod)
    )
  )

  implicit lazy val arbitraryLocalDateTime: Arbitrary[LocalDateTime] = Arbitrary(
    Gen.frequency((1, LocalDateTime.MIN), (1, LocalDateTime.MAX), (10, uniformLocalDateTime))
  )

  implicit lazy val arbitraryLocalDate: Arbitrary[LocalDate] = Arbitrary(
    Gen.frequency(
      (1, LocalDate.MIN),
      (1, LocalDate.MAX),
      (10, uniformLocalDate)
    )
  )

  implicit lazy val arbitraryInstant: Arbitrary[Instant] = Arbitrary(
    Gen.frequency(
      (1, Instant.EPOCH),
      (1, Instant.MAX),
      (1, Instant.MIN),
      (10, uniformInstant)
    )
  )

  implicit lazy val arbitaryTime: Arbitrary[LocalTime] = Arbitrary(
    Gen.frequency(
      (1, LocalTime.MAX),
      (1, LocalTime.MIN),
      (1, LocalTime.MIDNIGHT),
      (1, LocalTime.NOON),
      (10, uniformLocalTime)
    )
  )

  implicit lazy val arbitaryMonthDay: Arbitrary[MonthDay] = Arbitrary(uniformMonthDay)

  implicit lazy val arbitraryZoneId: Arbitrary[ZoneId] = Arbitrary(uniformZoneId)

  implicit lazy val arbitraryZoneOffset: Arbitrary[ZoneOffset] = Arbitrary(
    Gen.frequency(
      (1, ZoneOffset.MAX),
      (1, ZoneOffset.MIN),
      (1, ZoneOffset.UTC),
      (10, uniformZoneOffset)
    )
  )

  implicit lazy val arbitaryOffsetDateTime: Arbitrary[OffsetDateTime] = Arbitrary(
    Gen.frequency(
      (1, OffsetDateTime.MAX),
      (1, OffsetDateTime.MIN),
      (10, uniformOffsetDateTime)
    )
  )

  implicit lazy val arbitraryOffsetTime: Arbitrary[OffsetTime] = Arbitrary(
    Gen.frequency(
      (1, OffsetTime.MIN),
      (1, OffsetTime.MAX),
      (10, uniformOffsetTime)
    )
  )

  implicit lazy val artbitraryYear: Arbitrary[Year] = Arbitrary(
    Gen.frequency(
      (1, Gen.oneOf(Year.of(Year.MAX_VALUE), Year.of(Year.MIN_VALUE))),
      (10, uniformYear)
    )
  )

  implicit lazy val arbitraryYearMonth: Arbitrary[YearMonth] = Arbitrary(uniformYearMonth)

  implicit lazy val arbitraryZonedDateTime: Arbitrary[ZonedDateTime] = Arbitrary(
    Gen.frequency(
      (1, maxZonedDateTimes),
      (1, minZonedDateTimes),
      (10, uniformZonedDateTime)
    )
  )
}
