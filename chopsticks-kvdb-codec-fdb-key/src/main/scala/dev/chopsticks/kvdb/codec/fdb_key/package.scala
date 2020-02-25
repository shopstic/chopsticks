package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import com.apple.foundationdb.tuple.Tuple

package object fdb_key {
  implicit def fdbKeySerializer[T](
    implicit serializer: FdbKeySerializer[T]
  ): KeySerializer[T] = (key: T) => serializer.serialize(new Tuple(), key).pack()

  implicit def fdbKeyDeserializer[T](implicit deserializer: FdbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      deserializer.deserialize(new FdbTupleReader(Tuple.fromBytes(bytes)))
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
