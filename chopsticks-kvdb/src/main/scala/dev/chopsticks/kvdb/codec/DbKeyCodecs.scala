package dev.chopsticks.kvdb.codec

import java.time._

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}
import shapeless._

import scala.annotation.implicitNotFound
import scala.util.control.{NoStackTrace, NonFatal}

object DbKeyCodecs extends StrictLogging {
  sealed trait DecodingFailure extends NoStackTrace

  // scalastyle:off null
  final case class GenericDecodingException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)
      with DecodingFailure

  final case class WrongInputSizeException(message: String, targetName: String)
      extends RuntimeException(message)
      with DecodingFailure

  type DbKeyDecodeResult[T] = Either[DecodingFailure, T]

  @implicitNotFound(msg = "Implicit ToDbKey[${T}] not found. Try supplying an implicit instance of ToDbKey[${T}]")
  trait ToDbKey[T] {
    def encode(o: TupleOutput, t: T): TupleOutput
  }

  object ToDbKey extends ProductTypeClassCompanion[ToDbKey] {
    implicit val stringToDbKey: ToDbKey[String] = create((o, v) => o.writeString(v))
    implicit val booleanToDbKey: ToDbKey[Boolean] = create((o, v) => o.writeBoolean(v))
    implicit val byteToDbKey: ToDbKey[Byte] = create((o, v) => o.writeByte(v.toInt))
    implicit val shortToDbKey: ToDbKey[Short] = create((o, v) => o.writeShort(v.toInt))
    implicit val intToDbKey: ToDbKey[Int] = create((o, v) => o.writeInt(v))
    implicit val longToDbKey: ToDbKey[Long] = create((o, v) => o.writeLong(v))
    implicit val doubleToDbKey: ToDbKey[Double] = create((o, v) => o.writeSortedDouble(v))
    implicit val floatToDbKey: ToDbKey[Float] = create((o, v) => o.writeSortedFloat(v))
    implicit val hnilToDbKey: ToDbKey[HNil] = create((o, _) => o)

    implicit val ldToDbKey: ToDbKey[LocalDate] = create(
      (o, v) => longToDbKey.encode(o, ProtoMappers.localDateLongMapper.toBase(v))
    )
    implicit val ldtToDbKey: ToDbKey[LocalDateTime] = create { (o, v) =>
      o.writeBigInteger(KvdbSerdesUtils.localDateTimeToEpochNanos(v).underlying)
    }
    implicit val instantToDbKey: ToDbKey[Instant] = create { (o, v) =>
      o.writeBigInteger(KvdbSerdesUtils.instantToEpochNanos(v).underlying)
    }
    implicit val ltToDbKey: ToDbKey[LocalTime] = create(
      (o, v) => longToDbKey.encode(o, ProtoMappers.localTimeMapper.toBase(v))
    )
    implicit val ymToDbKey: ToDbKey[YearMonth] = create(
      (o, v) => longToDbKey.encode(o, ProtoMappers.yearMonthLongMapper.toBase(v))
    )
    implicit val bigDecimalToDbKey: ToDbKey[BigDecimal] = create((o, v) => o.writeSortedBigDecimal(v.underlying))

    def protobufValueWithMapperToDbKey[U, V](
      implicit underlyingEncoder: ToDbKey[U],
      mapper: scalapb.TypeMapper[U, V]
    ): ToDbKey[V] = { (o: TupleOutput, v: V) =>
      underlyingEncoder.encode(o, mapper.toBase(v))
    }

    implicit def protobufEnumToDbKey[T <: GeneratedEnum]: ToDbKey[T] = create((o, v) => intToDbKey.encode(o, v.value))

    def apply[V](implicit f: ToDbKey[V]): ToDbKey[V] = f

    def create[T](f: (TupleOutput, T) => TupleOutput): ToDbKey[T] = { (o: TupleOutput, t: T) =>
      f(o, t)
    }

    //noinspection MatchToPartialFunction
    implicit def deriveOption[T](implicit encoder: ToDbKey[T]): ToDbKey[Option[T]] = {
      create { (o, maybeValue) =>
        maybeValue match {
          case Some(v) => encoder.encode(o.writeBoolean(true), v)
          case None => o.writeBoolean(false)
        }
      }
    }

    object typeClass extends ProductTypeClass[ToDbKey] {

      val emptyProduct: ToDbKey[HNil] = hnilToDbKey

      def product[F, T <: HList](hc: ToDbKey[F], tc: ToDbKey[T]): ToDbKey[F :: T] = {
        create((out, hlist: F :: T) => {
          tc.encode(hc.encode(out, hlist.head), hlist.tail)
        })
      }

      def project[F, G](instance: => ToDbKey[G], to: F => G, from: G => F): ToDbKey[F] =
        create((o, f: F) => instance.encode(o, to(f)))
    }
  }

  @implicitNotFound(msg = "Implicit FromDbKey[${T}] not found. Try supplying an implicit instance of FromDbKey[${T}]")
  trait FromDbKey[T] {
    def decode(in: TupleInput): DbKeyDecodeResult[T]
  }

  object FromDbKey extends ProductTypeClassCompanion[FromDbKey] {
    import dev.chopsticks.kvdb.codec.ProtoMappers._

    def apply[V](implicit f: FromDbKey[V]): FromDbKey[V] = f

    private def createTry[T](f: TupleInput => T)(implicit typ: Typeable[T]): FromDbKey[T] = (in: TupleInput) => {
      try Right(f(in))
      catch {
        case NonFatal(e) => Left(GenericDecodingException(s"Failed decoding to ${typ.describe}: ${e.toString}", e))
      }
    }

    implicit val stringFromDbKey: FromDbKey[String] = createTry(_.readString)
    implicit val intFromDbKey: FromDbKey[Int] = createTry(_.readInt)
    implicit val longFromDbKey: FromDbKey[Long] = createTry(_.readLong)
    implicit val byteFromDbKey: FromDbKey[Byte] = createTry(_.readByte)
    implicit val shortFromDbKey: FromDbKey[Short] = createTry(_.readShort)
    implicit val doubleFromDbKey: FromDbKey[Double] = createTry(_.readSortedDouble)
    implicit val floatFromDbKey: FromDbKey[Float] = createTry(_.readSortedFloat)
    implicit val booleanFromDbKey: FromDbKey[Boolean] = createTry(_.readBoolean)

    implicit val ldFromDbKey: FromDbKey[LocalDate] = protobufValueWithMapperFromDbKey[Long, LocalDate]
    implicit val ldtFromDbKey: FromDbKey[LocalDateTime] = createTry { in: TupleInput =>
      KvdbSerdesUtils.epochNanosToLocalDateTime(BigInt(in.readBigInteger()))
    }
    implicit val ltFromDbKey: FromDbKey[LocalTime] = protobufValueWithMapperFromDbKey[Long, LocalTime]
    implicit val ymFromDbKey: FromDbKey[YearMonth] = protobufValueWithMapperFromDbKey[Long, YearMonth]

    implicit val instantFromDbKey: FromDbKey[Instant] = createTry { in: TupleInput =>
      KvdbSerdesUtils.epochNanosToInstant(BigInt(in.readBigInteger()))
    }

    implicit val bigDecimalFromDbKey: FromDbKey[BigDecimal] = createTry(in => BigDecimal(in.readSortedBigDecimal))

    def protobufValueWithMapperFromDbKey[U, V](
      implicit underlyingDecoder: FromDbKey[U],
      mapper: scalapb.TypeMapper[U, V],
      typ: Typeable[V]
    ): FromDbKey[V] = { in: TupleInput =>
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

    implicit def protobufEnumFromDbKey[T <: GeneratedEnum](
      implicit underlyingDecoder: FromDbKey[Int],
      comp: GeneratedEnumCompanion[T],
      typ: Typeable[T]
    ): FromDbKey[T] = { in: TupleInput =>
      underlyingDecoder.decode(in).flatMap { u =>
        try Right(comp.fromValue(u))
        catch {
          case NonFatal(e) =>
            Left(GenericDecodingException(s"Failed decoding to proto enum ${typ.describe}: ${e.toString}", e))
        }
      }
    }

    implicit def optionFromDbKey[T](
      implicit decoder: FromDbKey[T]
    ): FromDbKey[Option[T]] = { in: TupleInput =>
      for {
        hasValue <- booleanFromDbKey.decode(in)
        ret <- if (hasValue) decoder.decode(in).map(v => Some(v)) else Right(None)
      } yield ret
    }

    object typeClass extends ProductTypeClass[FromDbKey] {

      val emptyProduct: FromDbKey[HNil] = (_: TupleInput) => Right(HNil)

      def product[F, T <: HList](
        hc: FromDbKey[F],
        tc: FromDbKey[T]
      ): FromDbKey[F :: T] = { in =>
        for {
          head <- hc.decode(in)
          tail <- tc.decode(in)
        } yield head :: tail
      }

      def project[F, G](instance: => FromDbKey[G], to: F => G, from: G => F): FromDbKey[F] = { in =>
        instance.decode(in).map(from)
      }
    }
  }

}
