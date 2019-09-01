package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}
import java.util.UUID

import com.sleepycat.bind.tuple.TupleInput
import dev.chopsticks.kvdb.codec.KeyDeserializer.{GenericKeyDeserializationException, KeyDeserializationResult}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}
import shapeless.{::, HList, HNil, ProductTypeClass, ProductTypeClassCompanion, Typeable}

import scala.annotation.implicitNotFound
import scala.util.control.NonFatal
@implicitNotFound(
  msg = "Implicit BerkeleydbKeyDeserializer[${T}] not found. Try supplying an implicit instance of BerkeleydbKeyDeserializer[${T}]"
)
trait BerkeleydbKeyDeserializer[T] {
  def deserialize(in: TupleInput): KeyDeserializationResult[T]
}

object BerkeleydbKeyDeserializer extends ProductTypeClassCompanion[BerkeleydbKeyDeserializer] {
  def apply[V](implicit f: BerkeleydbKeyDeserializer[V]): BerkeleydbKeyDeserializer[V] = f

  private def createTry[T](f: TupleInput => T)(implicit typ: Typeable[T]): BerkeleydbKeyDeserializer[T] =
    (in: TupleInput) => {
      try Right(f(in))
      catch {
        case NonFatal(e) =>
          Left(GenericKeyDeserializationException(s"Failed decoding to ${typ.describe}: ${e.toString}", e))
      }
    }

  implicit val stringBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[String] = createTry(_.readString)
  implicit val intBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Int] = createTry(_.readInt)
  implicit val longBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Long] = createTry(_.readLong)
  implicit val byteBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Byte] = createTry(_.readByte)
  implicit val shortBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Short] = createTry(_.readShort)
  implicit val doubleBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Double] = createTry(_.readSortedDouble)
  implicit val floatBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Float] = createTry(_.readSortedFloat)
  implicit val booleanBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Boolean] = createTry(_.readBoolean)

  implicit val ldBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[LocalDate] = createTry(
    t => LocalDate.ofEpochDay(t.readLong())
  )
  implicit val ldtBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[LocalDateTime] = createTry(
    t => KvdbSerdesUtils.epochNanosToLocalDateTime(BigInt(t.readBigInteger()))
  )
  implicit val ltBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[LocalTime] = createTry(
    t => LocalTime.ofNanoOfDay(t.readLong())
  )
  implicit val ymBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[YearMonth] = createTry { t =>
    val v = t.readLong()

    if (v == 0) YearMonth.of(0, 1)
    else {
      val year = v / 100
      val month = v - year * 100
      YearMonth.of(year.toInt, month.toInt)
    }
  }

  implicit val instantBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[Instant] = createTry { in: TupleInput =>
    KvdbSerdesUtils.epochNanosToInstant(BigInt(in.readBigInteger()))
  }

  implicit val bigDecimalBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[BigDecimal] = createTry(
    in => BigDecimal(in.readSortedBigDecimal)
  )
  implicit val uuidBerkeleydbKeyDecoder: BerkeleydbKeyDeserializer[UUID] = createTry(
    in => new UUID(in.readLong(), in.readLong())
  )

  implicit def protobufEnumBerkeleydbKeyDecoder[T <: GeneratedEnum](
    implicit underlyingDecoder: BerkeleydbKeyDeserializer[Int],
    comp: GeneratedEnumCompanion[T],
    typ: Typeable[T]
  ): BerkeleydbKeyDeserializer[T] = { in: TupleInput =>
    underlyingDecoder.deserialize(in).flatMap { u =>
      try Right(comp.fromValue(u))
      catch {
        case NonFatal(e) =>
          Left(GenericKeyDeserializationException(s"Failed decoding to proto enum ${typ.describe}: ${e.toString}", e))
      }
    }
  }

  implicit def optionBerkeleydbKeyDecoder[T](
    implicit decoder: BerkeleydbKeyDeserializer[T]
  ): BerkeleydbKeyDeserializer[Option[T]] = { in: TupleInput =>
    for {
      hasValue <- booleanBerkeleydbKeyDecoder.deserialize(in)
      ret <- if (hasValue) decoder.deserialize(in).map(v => Some(v)) else Right(None)
    } yield ret
  }

  object typeClass extends ProductTypeClass[BerkeleydbKeyDeserializer] {

    val emptyProduct: BerkeleydbKeyDeserializer[HNil] = (_: TupleInput) => Right(HNil)

    def product[F, T <: HList](
      hc: BerkeleydbKeyDeserializer[F],
      tc: BerkeleydbKeyDeserializer[T]
    ): BerkeleydbKeyDeserializer[F :: T] = { in =>
      for {
        head <- hc.deserialize(in)
        tail <- tc.deserialize(in)
      } yield head :: tail
    }

    def project[F, G](
      instance: => BerkeleydbKeyDeserializer[G],
      to: F => G,
      from: G => F
    ): BerkeleydbKeyDeserializer[F] = { in =>
      instance.deserialize(in).map(from)
    }
  }
}
