package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}
import shapeless.HNil

//noinspection TypeAnnotation
package object berkeleydb_key {
  implicit def berkeleydbKeySerializer[T](implicit
    serializer: BerkeleydbKeySerializer[T]
  ): KeySerializer[T] = (key: T) => serializer.serialize(new TupleOutput(), key).toByteArray

  implicit def berkeleydbKeyPrefixSerializer[T](implicit
    serializer: BerkeleydbKeyPrefixSerializer[T]
  ): KeyPrefixSerializer[T] = {
    new KeyPrefixSerializer[T] {
      override def serializePrefix[P](prefix: P)(implicit ev: KeyPrefix[P, T]): Array[Byte] = {
        val flattened = ev.flatten(prefix)
        serializer.serializePrefix(new TupleOutput(), flattened) match {
          case (output, HNil) => output.toByteArray
          case (_, leftover) => throw new IllegalStateException(
              s"Prefix has not been fully consumed during serialization. Leftovers: $leftover"
            )
        }
      }
    }
  }

  implicit def berkeleydbKeyDeserializer[T](implicit deserializer: BerkeleydbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] => deserializer.deserialize(new TupleInput(bytes))
  }

  implicit lazy val stringKeySerdes = KeySerdes[String]
  implicit lazy val booleanKeySerdes = KeySerdes[Boolean]
  implicit lazy val byteKeySerdes = KeySerdes[Byte]
  implicit lazy val shortKeySerdes = KeySerdes[Short]
  implicit lazy val intKeySerdes = KeySerdes[Int]
  implicit lazy val longKeySerdes = KeySerdes[Long]
  implicit lazy val doubleKeySerdes = KeySerdes[Double]
  implicit lazy val floatKeySerdes = KeySerdes[Float]

  implicit lazy val ldKeySerdes = KeySerdes[LocalDate]
  implicit lazy val ldtKeySerdes = KeySerdes[LocalDateTime]
  implicit lazy val instantKeySerdes = KeySerdes[Instant]
  implicit lazy val ltKeySerdes = KeySerdes[LocalTime]
  implicit lazy val ymKeySerdes = KeySerdes[YearMonth]
  implicit lazy val bigDecimalKeySerdes = KeySerdes[BigDecimal]
  implicit lazy val uuidKeySerdes = KeySerdes[UUID]
}
