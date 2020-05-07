package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import dev.chopsticks.kvdb.codec.KeyDeserializer.GenericKeyDeserializationException

import scala.util.control.NonFatal

package object fdb_key {
  implicit def fdbKeySerializer[T](implicit
    serializer: FdbKeySerializer[T]
  ): KeySerializer[T] = (key: T) => serializer.serialize(new Tuple(), key).pack()

  implicit def fdbKeyDeserializer[T](implicit deserializer: FdbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      {
        val reader =
          try {
            new FdbTupleReader(Tuple.fromBytes(bytes))
          }
          catch {
            case NonFatal(e) =>
              throw GenericKeyDeserializationException(
                s"""
               |Failed decoding tuple: ${e.toString}
               |------------------------------------------------
               |Raw bytes: ${ByteArrayUtil.printable(bytes)}
               |------------------------------------------------
               |""".stripMargin,
                e
              )
          }

        deserializer.deserialize(reader)
      }
  }

  implicit val stringKeySerdes = KeySerdes[String]
  implicit val booleanKeySerdes = KeySerdes[Boolean]
  implicit val byteKeySerdes = KeySerdes[Byte]
  implicit val shortKeySerdes = KeySerdes[Short]
  implicit val intKeySerdes = KeySerdes[Int]
  implicit val longKeySerdes = KeySerdes[Long]
  implicit val doubleKeySerdes = KeySerdes[Double]
  implicit val floatKeySerdes = KeySerdes[Float]

  implicit val ldKeySerdes = KeySerdes[LocalDate]
  implicit val ldtKeySerdes = KeySerdes[LocalDateTime]
  implicit val instantKeySerdes = KeySerdes[Instant]
  implicit val ltKeySerdes = KeySerdes[LocalTime]
  implicit val ymKeySerdes = KeySerdes[YearMonth]
  implicit val uuidKeySerdes = KeySerdes[UUID]
}
