package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.sleepycat.bind.tuple.TupleOutput
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import enumeratum.EnumEntry
import enumeratum.values.{ByteEnumEntry, IntEnumEntry, ShortEnumEntry}
import magnolia._
import scalapb.GeneratedEnum

import scala.annotation.implicitNotFound
import scala.language.experimental.macros

@implicitNotFound(
  msg = "Implicit BerkeleydbKeySerializer[${T}] not found. Try supplying an implicit instance of BerkeleydbKeySerializer[${T}]"
)
trait BerkeleydbKeySerializer[T] {
  def serialize(o: TupleOutput, t: T): TupleOutput
}

object BerkeleydbKeySerializer {
  type Typeclass[A] = BerkeleydbKeySerializer[A]

  implicit val stringBerkeleydbKeyEncoder: BerkeleydbKeySerializer[String] = create((o, v) => o.writeString(v))
  implicit val booleanBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Boolean] = create((o, v) => o.writeBoolean(v))
  implicit val byteBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Byte] = create((o, v) => o.writeByte(v.toInt))
  implicit val shortBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Short] = create((o, v) => o.writeShort(v.toInt))
  implicit val intBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Int] = create((o, v) => o.writeInt(v))
  implicit val longBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Long] = create((o, v) => o.writeLong(v))
  implicit val doubleBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Double] = create((o, v) => o.writeSortedDouble(v))
  implicit val floatBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Float] = create((o, v) => o.writeSortedFloat(v))

  implicit val ldBerkeleydbKeyEncoder: BerkeleydbKeySerializer[LocalDate] = create(
    (o, v) => longBerkeleydbKeyEncoder.serialize(o, v.toEpochDay)
  )
  implicit val ldtBerkeleydbKeyEncoder: BerkeleydbKeySerializer[LocalDateTime] = create { (o, v) =>
    o.writeBigInteger(KvdbSerdesUtils.localDateTimeToEpochNanos(v).underlying)
  }
  implicit val instantBerkeleydbKeyEncoder: BerkeleydbKeySerializer[Instant] = create { (o, v) =>
    o.writeBigInteger(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltBerkeleydbKeyEncoder: BerkeleydbKeySerializer[LocalTime] = create(
    (o, v) => longBerkeleydbKeyEncoder.serialize(o, v.toNanoOfDay)
  )
  implicit val ymBerkeleydbKeyEncoder: BerkeleydbKeySerializer[YearMonth] = create(
    (o, v) => longBerkeleydbKeyEncoder.serialize(o, v.getYear.toLong * 100 + v.getMonthValue)
  )
  implicit val bigDecimalBerkeleydbKeyEncoder: BerkeleydbKeySerializer[BigDecimal] = create(
    (o, v) => o.writeSortedBigDecimal(v.underlying)
  )
  implicit val uuidBerkeleydbKeyEncoder: BerkeleydbKeySerializer[UUID] = create(
    (o, v) => o.writeLong(v.getMostSignificantBits).writeLong(v.getLeastSignificantBits)
  )

  implicit def protobufEnumBerkeleydbKeyEncoder[T <: GeneratedEnum]: BerkeleydbKeySerializer[T] =
    create((o, v) => intBerkeleydbKeyEncoder.serialize(o, v.value))

  implicit def enumeratumByteEnumKeyEncoder[E <: ByteEnumEntry]: BerkeleydbKeySerializer[E] =
    (o: TupleOutput, t: E) => o.writeByte(t.value.toInt)

  implicit def enumeratumShortEnumKeyEncoder[E <: ShortEnumEntry]: BerkeleydbKeySerializer[E] =
    (o: TupleOutput, t: E) => o.writeShort(t.value.toInt)

  implicit def enumeratumIntEnumKeyEncoder[E <: IntEnumEntry]: BerkeleydbKeySerializer[E] =
    (o: TupleOutput, t: E) => o.writeInt(t.value)

  implicit def enumeratumEnumKeyEncoder[E <: EnumEntry]: BerkeleydbKeySerializer[E] =
    (o: TupleOutput, t: E) => o.writeString(t.entryName)

  def apply[V](implicit f: BerkeleydbKeySerializer[V]): BerkeleydbKeySerializer[V] = f

  def create[T](f: (TupleOutput, T) => TupleOutput): BerkeleydbKeySerializer[T] = { (o: TupleOutput, t: T) =>
    f(o, t)
  }

  def combine[A](ctx: CaseClass[BerkeleydbKeySerializer, A]): BerkeleydbKeySerializer[A] =
    (o: TupleOutput, a: A) =>
      ctx.parameters.foldLeft(o) { (tuple, p) =>
        p.typeclass.serialize(tuple, p.dereference(a))
      }

  //noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit encoder: BerkeleydbKeySerializer[T]): BerkeleydbKeySerializer[Option[T]] = {
    create { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.serialize(o.writeBoolean(true), v)
        case None => o.writeBoolean(false)
      }
    }
  }

  implicit def deriveSerializer[A]: BerkeleydbKeySerializer[A] = macro Magnolia.gen[A]
}
