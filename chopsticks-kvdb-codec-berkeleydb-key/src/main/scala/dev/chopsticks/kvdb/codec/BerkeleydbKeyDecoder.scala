package dev.chopsticks.kvdb.codec

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, YearMonth}

import com.sleepycat.bind.tuple.TupleInput
import dev.chopsticks.kvdb.codec.DbKeyDecoder.{DbKeyDecodeResult, GenericDecodingException}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}
import shapeless.{::, HList, HNil, ProductTypeClass, ProductTypeClassCompanion, Typeable}

import scala.annotation.implicitNotFound
import scala.util.control.NonFatal


@implicitNotFound(msg = "Implicit BerkeleydbKeyDecoder[${T}] not found. Try supplying an implicit instance of BerkeleydbKeyDecoder[${T}]")
trait BerkeleydbKeyDecoder[T] {
  def decode(in: TupleInput): DbKeyDecodeResult[T]
}

object BerkeleydbKeyDecoder extends ProductTypeClassCompanion[BerkeleydbKeyDecoder] {
  import dev.chopsticks.kvdb.codec.ProtoMappers._

  def apply[V](implicit f: BerkeleydbKeyDecoder[V]): BerkeleydbKeyDecoder[V] = f

  private def createTry[T](f: TupleInput => T)(implicit typ: Typeable[T]): BerkeleydbKeyDecoder[T] = (in: TupleInput) => {
    try Right(f(in))
    catch {
      case NonFatal(e) => Left(GenericDecodingException(s"Failed decoding to ${typ.describe}: ${e.toString}", e))
    }
  }

  implicit val stringBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[String] = createTry(_.readString)
  implicit val intBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Int] = createTry(_.readInt)
  implicit val longBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Long] = createTry(_.readLong)
  implicit val byteBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Byte] = createTry(_.readByte)
  implicit val shortBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Short] = createTry(_.readShort)
  implicit val doubleBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Double] = createTry(_.readSortedDouble)
  implicit val floatBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Float] = createTry(_.readSortedFloat)
  implicit val booleanBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Boolean] = createTry(_.readBoolean)

  implicit val ldBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[LocalDate] = protobufValueWithMapperBerkeleydbKeyDecoder[Long, LocalDate]
  implicit val ldtBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[LocalDateTime] = createTry { in: TupleInput =>
    KvdbSerdesUtils.epochNanosToLocalDateTime(BigInt(in.readBigInteger()))
  }
  implicit val ltBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[LocalTime] = protobufValueWithMapperBerkeleydbKeyDecoder[Long, LocalTime]
  implicit val ymBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[YearMonth] = protobufValueWithMapperBerkeleydbKeyDecoder[Long, YearMonth]

  implicit val instantBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[Instant] = createTry { in: TupleInput =>
    KvdbSerdesUtils.epochNanosToInstant(BigInt(in.readBigInteger()))
  }

  implicit val bigDecimalBerkeleydbKeyDecoder: BerkeleydbKeyDecoder[BigDecimal] = createTry(in => BigDecimal(in.readSortedBigDecimal))

  def protobufValueWithMapperBerkeleydbKeyDecoder[U, V](
    implicit underlyingDecoder: BerkeleydbKeyDecoder[U],
    mapper: scalapb.TypeMapper[U, V],
    typ: Typeable[V]
  ): BerkeleydbKeyDecoder[V] = { in: TupleInput =>
    underlyingDecoder.decode(in).flatMap { u =>
      try Right(mapper.toCustom(u))
      catch {
        case NonFatal(e) =>
          Left(
            GenericDecodingException(
              s"Failed decoding to proto value ${typ.describe} with proto mapper: ${e.toString}",
              e
            )
          )
      }
    }
  }

  implicit def protobufEnumBerkeleydbKeyDecoder[T <: GeneratedEnum](
    implicit underlyingDecoder: BerkeleydbKeyDecoder[Int],
    comp: GeneratedEnumCompanion[T],
    typ: Typeable[T]
  ): BerkeleydbKeyDecoder[T] = { in: TupleInput =>
    underlyingDecoder.decode(in).flatMap { u =>
      try Right(comp.fromValue(u))
      catch {
        case NonFatal(e) =>
          Left(GenericDecodingException(s"Failed decoding to proto enum ${typ.describe}: ${e.toString}", e))
      }
    }
  }

  implicit def optionBerkeleydbKeyDecoder[T](
    implicit decoder: BerkeleydbKeyDecoder[T]
  ): BerkeleydbKeyDecoder[Option[T]] = { in: TupleInput =>
    for {
      hasValue <- booleanBerkeleydbKeyDecoder.decode(in)
      ret <- if (hasValue) decoder.decode(in).map(v => Some(v)) else Right(None)
    } yield ret
  }

  object typeClass extends ProductTypeClass[BerkeleydbKeyDecoder] {

    val emptyProduct: BerkeleydbKeyDecoder[HNil] = (_: TupleInput) => Right(HNil)

    def product[F, T <: HList](
      hc: BerkeleydbKeyDecoder[F],
      tc: BerkeleydbKeyDecoder[T]
    ): BerkeleydbKeyDecoder[F :: T] = { in =>
      for {
        head <- hc.decode(in)
        tail <- tc.decode(in)
      } yield head :: tail
    }

    def project[F, G](instance: => BerkeleydbKeyDecoder[G], to: F => G, from: G => F): BerkeleydbKeyDecoder[F] = { in =>
      instance.decode(in).map(from)
    }
  }
}
