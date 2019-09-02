package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}

//noinspection TypeAnnotation
package object berkeleydb_key {
  implicit def berkeleydbKeySerializer[T](
    implicit serializer: BerkeleydbKeySerializer[T]
  ): KeySerializer.Aux[T, BerkeleyDbKeyCodec] = new KeySerializer[T] {
    type Codec = BerkeleyDbKeyCodec
    def serialize(key: T): Array[Byte] = serializer.serialize(new TupleOutput(), key).toByteArray
  }

  implicit def berkeleydbKeyDeserializer[T](implicit deserializer: BerkeleydbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      deserializer.deserialize(new TupleInput(bytes))
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
  implicit val bigDecimalKeySerdes = KeySerdes[BigDecimal]
  implicit val uuidKeySerdes = KeySerdes[UUID]
}
