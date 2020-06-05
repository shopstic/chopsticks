package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import enumeratum.EnumEntry
import enumeratum.values.{ByteEnumEntry, IntEnumEntry, ShortEnumEntry}
import eu.timepit.refined.api.{RefType, Validate}
import magnolia._
import scalapb.GeneratedEnum

import scala.annotation.{implicitNotFound, nowarn}
import scala.language.experimental.macros
@implicitNotFound(
  msg = "Implicit FdbKeySerializer[${T}] not found. Try supplying an implicit instance of FdbKeySerializer[${T}]"
)
trait FdbKeySerializer[T] {
  def serialize(o: Tuple, t: T): Tuple
}

object FdbKeySerializer {
  type Typeclass[A] = FdbKeySerializer[A]

  implicit val stringFdbKeyEncoder: FdbKeySerializer[String] = create((o, v) => o.add(v))
  implicit val booleanFdbKeyEncoder: FdbKeySerializer[Boolean] = create((o, v) => o.add(v))
  implicit val byteFdbKeyEncoder: FdbKeySerializer[Byte] = create((o, v) => o.add(v.toLong))
  implicit val shortFdbKeyEncoder: FdbKeySerializer[Short] = create((o, v) => o.add(v.toLong))
  implicit val intFdbKeyEncoder: FdbKeySerializer[Int] = create((o, v) => o.add(v.toLong))
  implicit val longFdbKeyEncoder: FdbKeySerializer[Long] = create((o, v) => o.add(v))
  implicit val doubleFdbKeyEncoder: FdbKeySerializer[Double] = create((o, v) => o.add(v))
  implicit val floatFdbKeyEncoder: FdbKeySerializer[Float] = create((o, v) => o.add(v))

  implicit val ldFdbKeyEncoder: FdbKeySerializer[LocalDate] =
    create((o, v) => longFdbKeyEncoder.serialize(o, v.toEpochDay))
  implicit val ldtFdbKeyEncoder: FdbKeySerializer[LocalDateTime] = create { (o, v) =>
    o.add(KvdbSerdesUtils.localDateTimeToEpochNanos(v).underlying)
  }
  implicit val instantFdbKeyEncoder: FdbKeySerializer[Instant] = create { (o, v) =>
    o.add(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltFdbKeyEncoder: FdbKeySerializer[LocalTime] =
    create((o, v) => longFdbKeyEncoder.serialize(o, v.toNanoOfDay))
  implicit val ymFdbKeyEncoder: FdbKeySerializer[YearMonth] =
    create((o, v) => longFdbKeyEncoder.serialize(o, v.getYear.toLong * 100 + v.getMonthValue))
  implicit val uuidFdbKeyEncoder: FdbKeySerializer[UUID] =
    create((o, v) => o.add(v))
  implicit val versionStampFdbKeyEncoder: FdbKeySerializer[Versionstamp] =
    create((o, v) => o.add(v))

  implicit def protobufEnumFdbKeyEncoder[T <: GeneratedEnum]: FdbKeySerializer[T] =
    create((o, v) => intFdbKeyEncoder.serialize(o, v.value))

  implicit def enumeratumByteEnumKeyEncoder[E <: ByteEnumEntry]: FdbKeySerializer[E] =
    (o: Tuple, t: E) => o.add(t.value.toLong)

  implicit def enumeratumShortEnumKeyEncoder[E <: ShortEnumEntry]: FdbKeySerializer[E] =
    (o: Tuple, t: E) => o.add(t.value.toLong)

  implicit def enumeratumIntEnumKeyEncoder[E <: IntEnumEntry]: FdbKeySerializer[E] =
    (o: Tuple, t: E) => o.add(t.value.toLong)

  implicit def enumeratumEnumKeyEncoder[E <: EnumEntry]: FdbKeySerializer[E] =
    (o: Tuple, t: E) => o.add(t.entryName)

  implicit def refinedFdbKeySerializer[F[_, _], T, P](implicit
    serializer: FdbKeySerializer[T],
    refType: RefType[F],
    @nowarn validate: Validate[T, P]
  ): FdbKeySerializer[F[T, P]] = {
    (o: Tuple, t: F[T, P]) => serializer.serialize(o, refType.unwrap(t))
  }

  def apply[V](implicit f: FdbKeySerializer[V]): FdbKeySerializer[V] = f

  def create[T](f: (Tuple, T) => Tuple): FdbKeySerializer[T] = { (o: Tuple, t: T) => f(o, t) }

  def combine[A](ctx: CaseClass[FdbKeySerializer, A]): FdbKeySerializer[A] =
    (o: Tuple, a: A) => ctx.parameters.foldLeft(o) { (tuple, p) => p.typeclass.serialize(tuple, p.dereference(a)) }

  def dispatch[A](ctx: SealedTrait[FdbKeySerializer, A]): FdbKeySerializer[A] = (o: Tuple, a: A) => {
    ctx.dispatch(a) { sub: Subtype[Typeclass, A] =>
      sub.typeclass.serialize(o.add(sub.index.toLong), sub.cast(a))
    }
  }

  //noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit encoder: FdbKeySerializer[T]): FdbKeySerializer[Option[T]] = {
    create { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.serialize(o.add(true), v)
        case None => o.add(false)
      }
    }
  }

  implicit def deriveSerializer[A]: FdbKeySerializer[A] = macro Magnolia.gen[A]
}
