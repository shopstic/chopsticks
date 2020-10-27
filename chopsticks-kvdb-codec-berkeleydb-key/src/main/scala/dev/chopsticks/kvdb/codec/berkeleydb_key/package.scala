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
      override def serializePrefix[P](prefix: P)(implicit ev: KeyPrefixEvidence[P, T]): Array[Byte] = {
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
