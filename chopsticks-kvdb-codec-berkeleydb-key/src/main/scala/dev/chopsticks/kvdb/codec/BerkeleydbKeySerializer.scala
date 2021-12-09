package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.sleepycat.bind.tuple.TupleOutput
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import enumeratum.EnumEntry
import enumeratum.values.{ByteEnumEntry, IntEnumEntry, ShortEnumEntry}
import eu.timepit.refined.api.{RefType, Validate}
import magnolia._
import scalapb.GeneratedEnum

import scala.annotation.{implicitNotFound, nowarn}
import scala.language.experimental.macros

@implicitNotFound(
  msg =
    "Implicit BerkeleydbKeySerializer[${T}] not found. Try supplying an implicit instance of BerkeleydbKeySerializer[${T}]"
)
trait BerkeleydbKeySerializer[T] {
  def serialize(tupleOutput: TupleOutput, value: T): TupleOutput
}

trait PredefinedBerkeleydbKeySerializer[T] extends BerkeleydbKeySerializer[T]
trait AutoBerkeleydbKeySerializer[T] extends BerkeleydbKeySerializer[T]

object BerkeleydbKeySerializer {
  type Typeclass[A] = BerkeleydbKeySerializer[A]

  implicit val stringBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[String] =
    define((o, v) => o.writeString(v))
  implicit val booleanBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Boolean] =
    define((o, v) => o.writeBoolean(v))
  implicit val byteBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Byte] =
    define((o, v) => o.writeByte(v.toInt))
  implicit val shortBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Short] =
    define((o, v) => o.writeShort(v.toInt))
  implicit val intBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Int] = define((o, v) => o.writeInt(v))
  implicit val longBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Long] = define((o, v) => o.writeLong(v))
  implicit val doubleBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Double] =
    define((o, v) => o.writeSortedDouble(v))
  implicit val floatBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Float] =
    define((o, v) => o.writeSortedFloat(v))

  implicit val ldBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[LocalDate] =
    define((o, v) => longBerkeleydbKeyEncoder.serialize(o, v.toEpochDay))
  implicit val instantBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[Instant] = define { (o, v) =>
    o.writeBigInteger(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[LocalTime] =
    define((o, v) => longBerkeleydbKeyEncoder.serialize(o, v.toNanoOfDay))
  implicit val ymBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[YearMonth] =
    define((o, v) => longBerkeleydbKeyEncoder.serialize(o, v.getYear.toLong * 100 + v.getMonthValue))
  implicit val bigDecimalBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[BigDecimal] =
    define((o, v) => o.writeSortedBigDecimal(v.underlying))
  implicit val uuidBerkeleydbKeyEncoder: PredefinedBerkeleydbKeySerializer[UUID] =
    define((o, v) => o.writeLong(v.getMostSignificantBits).writeLong(v.getLeastSignificantBits))

  implicit def protobufEnumBerkeleydbKeyEncoder[T <: GeneratedEnum]: PredefinedBerkeleydbKeySerializer[T] =
    define((o, v) => intBerkeleydbKeyEncoder.serialize(o, v.value))

  implicit def enumeratumByteEnumKeyEncoder[E <: ByteEnumEntry]: PredefinedBerkeleydbKeySerializer[E] =
    define((o, v) => o.writeByte(v.value.toInt))

  implicit def enumeratumShortEnumKeyEncoder[E <: ShortEnumEntry]: PredefinedBerkeleydbKeySerializer[E] =
    define((o, v) => o.writeShort(v.value.toInt))

  implicit def enumeratumIntEnumKeyEncoder[E <: IntEnumEntry]: PredefinedBerkeleydbKeySerializer[E] =
    define((o, v) => o.writeInt(v.value))

  implicit def enumeratumEnumKeyEncoder[E <: EnumEntry]: PredefinedBerkeleydbKeySerializer[E] =
    define((o, v) => o.writeString(v.entryName))

  implicit def refinedBerkeleydbKeySerializer[F[_, _], A, B](implicit
    serializer: BerkeleydbKeySerializer[A],
    refType: RefType[F],
    @nowarn validate: Validate[A, B]
  ): PredefinedBerkeleydbKeySerializer[F[A, B]] =
    define((tupleOutput: TupleOutput, value: F[A, B]) => serializer.serialize(tupleOutput, refType.unwrap(value)))

  // noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit
    encoder: BerkeleydbKeySerializer[T]
  ): PredefinedBerkeleydbKeySerializer[Option[T]] = {
    define { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.serialize(o.writeBoolean(true), v)
        case None => o.writeBoolean(false)
      }
    }
  }

  def define[T](ser: (TupleOutput, T) => TupleOutput): PredefinedBerkeleydbKeySerializer[T] =
    (tupleOutput: TupleOutput, value: T) => ser(tupleOutput, value)

  def apply[V](implicit f: BerkeleydbKeySerializer[V]): BerkeleydbKeySerializer[V] = f

  def combine[T](ctx: CaseClass[BerkeleydbKeySerializer, T]): AutoBerkeleydbKeySerializer[T] =
    (tupleOutput: TupleOutput, value: T) => {
      ctx.parameters.foldLeft(tupleOutput) { (tuple, p) => p.typeclass.serialize(tuple, p.dereference(value)) }
    }

  implicit def deriveSerializer[A]: BerkeleydbKeySerializer[A] = macro Magnolia.gen[A]
}
