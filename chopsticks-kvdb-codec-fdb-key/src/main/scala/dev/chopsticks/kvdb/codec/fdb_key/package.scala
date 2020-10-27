package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}
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
      override def serializePrefix[P](prefix: P)(implicit ev: KeyPrefixEvidence[P, T]): Array[Byte] = {
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
  implicit val nonEmptyStringSerdes = KeySerdes[NonEmptyString]
  implicit val posIntSerdes = KeySerdes[PosInt]
  implicit val posLongSerdes = KeySerdes[PosLong]
}
