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
import scala.reflect.ClassTag

@implicitNotFound(
  msg = "Implicit FdbKeySerializer[${T}] not found. Try supplying an implicit instance of FdbKeySerializer[${T}]"
)
trait FdbKeySerializer[T] {
  def serialize(o: Tuple, t: T): Tuple
}

trait PredefinedFdbKeySerializer[T] extends FdbKeySerializer[T]
trait CaseClassFdbKeySerializer[T] extends FdbKeySerializer[T]
trait SealedTraitFdbKeySerializer[T] extends FdbKeySerializer[T]

object FdbKeySerializer {
  type Typeclass[A] = FdbKeySerializer[A]

  implicit val stringFdbKeyEncoder: PredefinedFdbKeySerializer[String] = define((o, v) => o.add(v))
  implicit val booleanFdbKeyEncoder: PredefinedFdbKeySerializer[Boolean] = define((o, v) => o.add(v))
  implicit val byteFdbKeyEncoder: PredefinedFdbKeySerializer[Byte] = define((o, v) => o.add(v.toLong))
  implicit val shortFdbKeyEncoder: PredefinedFdbKeySerializer[Short] = define((o, v) => o.add(v.toLong))
  implicit val intFdbKeyEncoder: PredefinedFdbKeySerializer[Int] = define((o, v) => o.add(v.toLong))
  implicit val longFdbKeyEncoder: PredefinedFdbKeySerializer[Long] = define((o, v) => o.add(v))
  implicit val doubleFdbKeyEncoder: PredefinedFdbKeySerializer[Double] = define((o, v) => o.add(v))
  implicit val floatFdbKeyEncoder: PredefinedFdbKeySerializer[Float] = define((o, v) => o.add(v))

  implicit val ldFdbKeyEncoder: PredefinedFdbKeySerializer[LocalDate] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.toEpochDay))
  implicit val instantFdbKeyEncoder: PredefinedFdbKeySerializer[Instant] = define { (o, v) =>
    o.add(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltFdbKeyEncoder: PredefinedFdbKeySerializer[LocalTime] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.toNanoOfDay))
  implicit val ymFdbKeyEncoder: PredefinedFdbKeySerializer[YearMonth] =
    define((o, v) => longFdbKeyEncoder.serialize(o, v.getYear.toLong * 100 + v.getMonthValue))
  implicit val uuidFdbKeyEncoder: PredefinedFdbKeySerializer[UUID] =
    define((o, v) => o.add(v))
  implicit val versionStampFdbKeyEncoder: PredefinedFdbKeySerializer[Versionstamp] =
    define((o, v) => o.add(v))

  implicit def protobufEnumFdbKeyEncoder[T <: GeneratedEnum: ClassTag]: PredefinedFdbKeySerializer[T] =
    define((o, v) => intFdbKeyEncoder.serialize(o, v.value))

  implicit def enumeratumByteEnumKeyEncoder[E <: ByteEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  implicit def enumeratumShortEnumKeyEncoder[E <: ShortEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  implicit def enumeratumIntEnumKeyEncoder[E <: IntEnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.value.toLong))

  implicit def enumeratumEnumKeyEncoder[E <: EnumEntry: ClassTag]: PredefinedFdbKeySerializer[E] =
    define((o: Tuple, t: E) => o.add(t.entryName))

  implicit def refinedFdbKeySerializer[F[_, _], T, P](implicit
    serializer: FdbKeySerializer[T],
    refType: RefType[F],
    @nowarn validate: Validate[T, P],
    @nowarn ct: ClassTag[F[T, P]]
  ): PredefinedFdbKeySerializer[F[T, P]] = {
    define((o: Tuple, t: F[T, P]) => serializer.serialize(o, refType.unwrap(t)))
  }

  def apply[V](implicit f: FdbKeySerializer[V]): FdbKeySerializer[V] = f

  def define[T](ser: (Tuple, T) => Tuple): PredefinedFdbKeySerializer[T] =
    (tupleOutput: Tuple, value: T) => ser(tupleOutput, value)

  def combine[T](ctx: CaseClass[FdbKeySerializer, T]): CaseClassFdbKeySerializer[T] =
    (tupleOutput: Tuple, value: T) => {
      ctx.parameters.foldLeft(tupleOutput) { (tuple, p) => p.typeclass.serialize(tuple, p.dereference(value)) }
    }

  def dispatch[A, Tag](ctx: SealedTrait[FdbKeySerializer, A])(implicit
    tag: FdbKeyCoproductTag.Aux[A, Tag],
    tagSerializer: FdbKeySerializer[Tag]
  ): SealedTraitFdbKeySerializer[A] =
    (tuple: Tuple, value: A) => {
      ctx.dispatch(value) { subType: Subtype[Typeclass, A] =>
        subType.typeclass.serialize(
          tagSerializer.serialize(tuple, tag.subTypeToTag(subType)),
          subType.cast(value)
        )
      }
    }

  //noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit encoder: FdbKeySerializer[T]): PredefinedFdbKeySerializer[Option[T]] = {
    define { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.serialize(o.add(true), v)
        case None => o.add(false)
      }
    }
  }

  implicit def deriveSerializer[A]: FdbKeySerializer[A] = macro Magnolia.gen[A]
}
