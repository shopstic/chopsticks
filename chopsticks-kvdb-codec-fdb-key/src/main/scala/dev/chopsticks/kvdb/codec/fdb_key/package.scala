package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalTime, YearMonth}
import java.util.UUID

import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import dev.chopsticks.kvdb.codec.KeyDeserializer.GenericKeyDeserializationException
import eu.timepit.refined.types.string.NonEmptyString

import scala.util.control.NonFatal
import eu.timepit.refined.shapeless.typeable._
import eu.timepit.refined.types.numeric.{PosInt, PosLong}
import shapeless.HNil

//noinspection TypeAnnotation
package object fdb_key {
  implicit def fdbKeySerializer[T](implicit
    serializer: FdbKeySerializer[T]
  ): KeySerializer[T] = (key: T) => {
    val tuple = serializer.serialize(new Tuple(), key)

    if (tuple.hasIncompleteVersionstamp) tuple.packWithVersionstamp()
    else tuple.pack()
  }

  implicit def fdbKeyPrefixSerializer[T](implicit
    serializer: FdbKeyPrefixSerializer[T]
  ): KeyPrefixSerializer[T] = {
    new KeyPrefixSerializer[T] {
      override def serializePrefix[P](prefix: P)(implicit ev: KeyPrefix[P, T]): Array[Byte] = {
        val flattened = ev.flatten(prefix)
        serializer.serializePrefix(new Tuple(), flattened) match {
          case (output, HNil) =>
            if (output.hasIncompleteVersionstamp) output.packWithVersionstamp()
            else output.pack()

          case (_, leftover) => throw new IllegalStateException(
              s"Prefix has not been fully consumed during serialization. Leftovers: $leftover"
            )
        }
      }
    }
  }
  implicit def fdbKeyDeserializer[T](implicit deserializer: FdbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      {
        val reader =
          try {
            Right(new FdbTupleReader(Tuple.fromBytes(bytes)))
          }
          catch {
            case NonFatal(e) =>
              Left(GenericKeyDeserializationException(
                s"""
                   |Failed decoding tuple: ${e.toString}
                   |------------------------------------------------
                   |Raw bytes: ${ByteArrayUtil.printable(bytes)}
                   |------------------------------------------------
                   |""".stripMargin,
                e
              ))
          }

        reader.flatMap(deserializer.deserialize)
      }
  }

  implicit lazy val byteArraySerdes = KeySerdes[Array[Byte]]
  implicit lazy val stringKeySerdes = KeySerdes[String]
  implicit lazy val booleanKeySerdes = KeySerdes[Boolean]
  implicit lazy val byteKeySerdes = KeySerdes[Byte]
  implicit lazy val shortKeySerdes = KeySerdes[Short]
  implicit lazy val intKeySerdes = KeySerdes[Int]
  implicit lazy val longKeySerdes = KeySerdes[Long]
  implicit lazy val doubleKeySerdes = KeySerdes[Double]
  implicit lazy val floatKeySerdes = KeySerdes[Float]

  implicit lazy val ldKeySerdes = KeySerdes[LocalDate]
  implicit lazy val instantKeySerdes = KeySerdes[Instant]
  implicit lazy val ltKeySerdes = KeySerdes[LocalTime]
  implicit lazy val ymKeySerdes = KeySerdes[YearMonth]
  implicit lazy val uuidKeySerdes = KeySerdes[UUID]
  implicit lazy val nonEmptyStringSerdes = KeySerdes[NonEmptyString]
  implicit lazy val posIntSerdes = KeySerdes[PosInt]
  implicit lazy val posLongSerdes = KeySerdes[PosLong]
}
