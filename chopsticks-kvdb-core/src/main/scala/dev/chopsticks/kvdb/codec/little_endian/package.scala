package dev.chopsticks.kvdb.codec

import java.nio.{ByteBuffer, ByteOrder}
import java.time.YearMonth

package object little_endian {
  implicit val longValueSerdes: ValueSerdes[Long] =
    ValueSerdes.create[Long](
      value => ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array(),
      bytes => Right(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong())
    )
  implicit val yearMonthValueSerdes: ValueSerdes[YearMonth] = longValueSerdes.bimap[YearMonth](v => {
    if (v == 0) YearMonth.of(0, 1)
    else {
      val year = v / 100
      val month = v - year * 100
      YearMonth.of(year.toInt, month.toInt)
    }
  })(ym => ym.getYear.toLong * 100 + ym.getMonthValue)
}
