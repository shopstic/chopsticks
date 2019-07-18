package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}

import com.sleepycat.bind.tuple.TupleOutput
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb.GeneratedEnum
import shapeless.{::, HList, HNil, ProductTypeClass, ProductTypeClassCompanion}

import scala.annotation.implicitNotFound

@implicitNotFound(
  msg =
    "Implicit BerkeleydbKeyEncoder[${T}] not found. Try supplying an implicit instance of BerkeleydbKeyEncoder[${T}]"
)
trait BerkeleydbKeyEncoder[T] {
  def encode(o: TupleOutput, t: T): TupleOutput
}

object BerkeleydbKeyEncoder extends ProductTypeClassCompanion[BerkeleydbKeyEncoder] {
  implicit val stringBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[String] = create((o, v) => o.writeString(v))
  implicit val booleanBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Boolean] = create((o, v) => o.writeBoolean(v))
  implicit val byteBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Byte] = create((o, v) => o.writeByte(v.toInt))
  implicit val shortBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Short] = create((o, v) => o.writeShort(v.toInt))
  implicit val intBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Int] = create((o, v) => o.writeInt(v))
  implicit val longBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Long] = create((o, v) => o.writeLong(v))
  implicit val doubleBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Double] = create((o, v) => o.writeSortedDouble(v))
  implicit val floatBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Float] = create((o, v) => o.writeSortedFloat(v))
  implicit val hnilBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[HNil] = create((o, _) => o)

  implicit val ldBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[LocalDate] = create(
    (o, v) => longBerkeleydbKeyEncoder.encode(o, ProtoMappers.localDateLongMapper.toBase(v))
  )
  implicit val ldtBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[LocalDateTime] = create { (o, v) =>
    o.writeBigInteger(KvdbSerdesUtils.localDateTimeToEpochNanos(v).underlying)
  }
  implicit val instantBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[Instant] = create { (o, v) =>
    o.writeBigInteger(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
  }
  implicit val ltBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[LocalTime] = create(
    (o, v) => longBerkeleydbKeyEncoder.encode(o, ProtoMappers.localTimeMapper.toBase(v))
  )
  implicit val ymBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[YearMonth] = create(
    (o, v) => longBerkeleydbKeyEncoder.encode(o, ProtoMappers.yearMonthLongMapper.toBase(v))
  )
  implicit val bigDecimalBerkeleydbKeyEncoder: BerkeleydbKeyEncoder[BigDecimal] = create(
    (o, v) => o.writeSortedBigDecimal(v.underlying)
  )

  def protobufValueWithMapperBerkeleydbKeyEncoder[U, V](
    implicit underlyingEncoder: BerkeleydbKeyEncoder[U],
    mapper: scalapb.TypeMapper[U, V]
  ): BerkeleydbKeyEncoder[V] = { (o: TupleOutput, v: V) =>
    underlyingEncoder.encode(o, mapper.toBase(v))
  }

  implicit def protobufEnumBerkeleydbKeyEncoder[T <: GeneratedEnum]: BerkeleydbKeyEncoder[T] =
    create((o, v) => intBerkeleydbKeyEncoder.encode(o, v.value))

  def apply[V](implicit f: BerkeleydbKeyEncoder[V]): BerkeleydbKeyEncoder[V] = f

  def create[T](f: (TupleOutput, T) => TupleOutput): BerkeleydbKeyEncoder[T] = { (o: TupleOutput, t: T) =>
    f(o, t)
  }

  //noinspection MatchToPartialFunction
  implicit def deriveOption[T](implicit encoder: BerkeleydbKeyEncoder[T]): BerkeleydbKeyEncoder[Option[T]] = {
    create { (o, maybeValue) =>
      maybeValue match {
        case Some(v) => encoder.encode(o.writeBoolean(true), v)
        case None => o.writeBoolean(false)
      }
    }
  }

  object typeClass extends ProductTypeClass[BerkeleydbKeyEncoder] {

    val emptyProduct: BerkeleydbKeyEncoder[HNil] = hnilBerkeleydbKeyEncoder

    def product[F, T <: HList](
      hc: BerkeleydbKeyEncoder[F],
      tc: BerkeleydbKeyEncoder[T]
    ): BerkeleydbKeyEncoder[F :: T] = {
      create((out, hlist: F :: T) => {
        tc.encode(hc.encode(out, hlist.head), hlist.tail)
      })
    }

    def project[F, G](instance: => BerkeleydbKeyEncoder[G], to: F => G, from: G => F): BerkeleydbKeyEncoder[F] =
      create((o, f: F) => instance.encode(o, to(f)))
  }
}
