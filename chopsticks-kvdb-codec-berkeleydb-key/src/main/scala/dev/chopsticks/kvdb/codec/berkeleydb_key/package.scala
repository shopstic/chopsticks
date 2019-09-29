package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}
import dev.chopsticks.kvdb.codec.KeyDeserializer.GenericKeyDeserializationException
import eu.timepit.refined.api.{RefType, Validate}

import scala.language.higherKinds

//noinspection TypeAnnotation
package object berkeleydb_key {
  implicit def berkeleydbKeySerializer[T](
    implicit serializer: BerkeleydbKeySerializer[T]
  ): KeySerializer[T] = (key: T) => serializer.serialize(new TupleOutput(), key).toByteArray

  implicit def berkeleydbKeyDeserializer[T](implicit deserializer: BerkeleydbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      deserializer.deserialize(new TupleInput(bytes))
  }

  implicit def refinedBerkeleydbKeySerializer[F[_, _], T, P](
    implicit serializer: BerkeleydbKeySerializer[T],
    refType: RefType[F],
    validate: Validate[T, P]
  ): BerkeleydbKeySerializer[F[T, P]] = {
    import dev.chopsticks.kvdb.util.UnusedImplicits._
    validate.unused()
    (o: TupleOutput, t: F[T, P]) => serializer.serialize(o, refType.unwrap(t))
  }


  implicit def refinedBerkeleydbKeyDeserializer[F[_, _], T, P](
    implicit deserializer: BerkeleydbKeyDeserializer[T],
    refType: RefType[F],
    validate: Validate[T, P]
  ): BerkeleydbKeyDeserializer[F[T, P]] = (in: TupleInput) => {
    import cats.syntax.either._
    deserializer.deserialize(in).flatMap { value =>
      refType.refine[P](value).leftMap(GenericKeyDeserializationException(_))
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
  implicit val bigDecimalKeySerdes = KeySerdes[BigDecimal]
  implicit val uuidKeySerdes = KeySerdes[UUID]
}
