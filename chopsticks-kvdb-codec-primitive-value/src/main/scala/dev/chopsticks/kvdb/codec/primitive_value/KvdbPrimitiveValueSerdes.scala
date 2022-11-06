package dev.chopsticks.kvdb.codec.primitive_value

import dev.chopsticks.kvdb.codec.ValueSerdes

import java.nio.{ByteBuffer, ByteOrder}

object KvdbPrimitiveValueSerdes {
  implicit val littleIndianIntValueSerdes: ValueSerdes[Int] =
    ValueSerdes.create[Int](
      value => ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array(),
      bytes => Right(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt())
    )

  implicit val littleIndianLongValueSerdes: ValueSerdes[Long] =
    ValueSerdes.create[Long](
      value => ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array(),
      bytes => Right(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong())
    )
}
