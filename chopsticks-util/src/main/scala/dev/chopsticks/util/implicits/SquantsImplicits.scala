package dev.chopsticks.util.implicits

import squants.time.*

object SquantsImplicits {
  private val nanosecond = 1.0d
  private val microsecondInNanos = Microseconds(1).toNanoseconds
  private val millisecondInNanos = Milliseconds(1).toNanoseconds
  private val secondInNanos = Seconds(1).toNanoseconds
  private val minuteInNanos = Minutes(1).toNanoseconds
  private val hourInNanos = Hours(1).toNanoseconds
  private val dayInNanos = Days(1).toNanoseconds

  extension (time: Time) {
    def inBestUnit: Time =
      val nanos = time.toNanoseconds
      if (nanos > dayInNanos) Days(time.to(Days))
      else if (nanos > hourInNanos) Hours(time.to(Hours))
      else if (nanos > minuteInNanos) Minutes(time.to(Minutes))
      else if (nanos > secondInNanos) Seconds(time.to(Seconds))
      else if (nanos > millisecondInNanos) Milliseconds(time.to(Milliseconds))
      else if (nanos > microsecondInNanos) Microseconds(time.to(Microseconds))
      else time
  }
}
