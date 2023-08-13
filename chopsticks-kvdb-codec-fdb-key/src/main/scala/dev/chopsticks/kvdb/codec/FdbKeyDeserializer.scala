package dev.chopsticks.kvdb.codec

import java.time._
import java.util.UUID

import com.apple.foundationdb.tuple.Versionstamp
import dev.chopsticks.kvdb.codec.KeyDeserializer.{DecodingFailure, GenericKeyDeserializationException}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import enumeratum.EnumEntry
import enumeratum.values._
import eu.timepit.refined.api.{RefType, Validate}
import magnolia._
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}
import shapeless.Typeable

import scala.annotation.implicitNotFound
import scala.collection.immutable.ArraySeq
import scala.language.experimental.macros
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@implicitNotFound(
  msg = "Implicit FdbKeyDeserializer[${T}] not found. Try supplying an implicit instance of FdbKeyDeserializer[${T}]"
)
trait FdbKeyDeserializer[T] {
  def deserialize(in: FdbTupleReader): Either[DecodingFailure, T]
}

object FdbKeyDeserializer {
  type Typeclass[A] = FdbKeyDeserializer[A]

  private[codec] def createTry[T](f: FdbTupleReader => T)(implicit typ: Typeable[T]): FdbKeyDeserializer[T] =
    (in: FdbTupleReader) => {
      try Right(f(in))
      catch {
        case NonFatal(e) =>
          Left(
            GenericKeyDeserializationException(
              s"""
                |Failed decoding to ${typ.describe}: ${e.toString}
                |------------------------------------------------
                |Details: ${in.toString}
                |------------------------------------------------
                |""".stripMargin,
              e
            )
          )
      }
    }

  implicit val byteArrayKeyDecoder: FdbKeyDeserializer[Array[Byte]] = createTry(_.getBytes)
  implicit val stringFdbKeyDecoder: FdbKeyDeserializer[String] = createTry(_.getString)
  implicit val intFdbKeyDecoder: FdbKeyDeserializer[Int] = createTry(_.getBigInteger.intValueExact())
  implicit val longFdbKeyDecoder: FdbKeyDeserializer[Long] = createTry(_.getBigInteger.longValueExact())
  implicit val byteFdbKeyDecoder: FdbKeyDeserializer[Byte] = createTry(_.getBigInteger.byteValueExact())
  implicit val shortFdbKeyDecoder: FdbKeyDeserializer[Short] = createTry(_.getBigInteger.shortValueExact())
  implicit val doubleFdbKeyDecoder: FdbKeyDeserializer[Double] = createTry(_.getDouble)
  implicit val floatFdbKeyDecoder: FdbKeyDeserializer[Float] = createTry(_.getFloat)
  implicit val booleanFdbKeyDecoder: FdbKeyDeserializer[Boolean] = createTry(_.getBoolean)

  implicit val byteArraySeqFdbKeyDecoder: FdbKeyDeserializer[ArraySeq[Byte]] =
    createTry(t => ArraySeq.unsafeWrapArray(t.getBytes))
  implicit val ldFdbKeyDecoder: FdbKeyDeserializer[LocalDate] = createTry { t =>
    val epochDay = t.getLong
    LocalDate.ofEpochDay(epochDay)
  }
  implicit val ltFdbKeyDecoder: FdbKeyDeserializer[LocalTime] = createTry { t =>
    val nanoOfDay = t.getLong
    LocalTime.ofNanoOfDay(nanoOfDay)
  }
  implicit val ymFdbKeyDecoder: FdbKeyDeserializer[YearMonth] = createTry { t =>
    val v = t.getLong

    if (v == 0) YearMonth.of(0, 1)
    else {
      val year = v / 100
      val month = v - year * 100
      YearMonth.of(year.toInt, month.toInt)
    }
  }

  implicit val instantFdbKeyDecoder: FdbKeyDeserializer[Instant] = createTry { t =>
    val epochNanos = t.getBigInteger
    KvdbSerdesUtils.epochNanosToInstant(BigInt(epochNanos))
  }

  implicit val uuidFdbKeyDecoder: FdbKeyDeserializer[UUID] = createTry(_.getUUID)
  implicit val versionstampFdbKeyDecoder: FdbKeyDeserializer[Versionstamp] = createTry(_.getVersionStamp)

  implicit def protobufEnumFdbKeyDecoder[T <: GeneratedEnum](implicit
    underlyingDecoder: FdbKeyDeserializer[Int],
    comp: GeneratedEnumCompanion[T],
    typ: Typeable[T]
  ): FdbKeyDeserializer[T] =
    (in: FdbTupleReader) =>
      underlyingDecoder.deserialize(in).flatMap { u =>
        try Right(comp.fromValue(u))
        catch {
          case NonFatal(e) =>
            Left(GenericKeyDeserializationException(s"Failed decoding to proto enum ${typ.describe}: ${e.toString}", e))
        }
      }

  implicit def enumeratumByteEnumKeyDecoder[E <: ByteEnumEntry](implicit
    e: ByteEnum[E]
  ): FdbKeyDeserializer[E] = (in: FdbTupleReader) => {
    Try(e.withValue(in.getBigInteger.byteValueExact())) match {
      case Failure(e) => Left(GenericKeyDeserializationException(e.getMessage, e))
      case Success(value) => Right(value)
    }
  }

  implicit def enumeratumShortEnumKeyDecoder[E <: ShortEnumEntry](implicit
    e: ShortEnum[E]
  ): FdbKeyDeserializer[E] = { (in: FdbTupleReader) =>
    Try(e.withValue(in.getBigInteger.shortValueExact())) match {
      case Failure(e) => Left(GenericKeyDeserializationException(e.getMessage, e))
      case Success(value) => Right(value)
    }
  }

  implicit def enumeratumIntEnumKeyDecoder[E <: IntEnumEntry](implicit
    e: IntEnum[E]
  ): FdbKeyDeserializer[E] = { (in: FdbTupleReader) =>
    Try(e.withValue(in.getBigInteger.intValueExact())) match {
      case Failure(e) => Left(GenericKeyDeserializationException(e.getMessage, e))
      case Success(value) => Right(value)
    }
  }

  implicit def enumeratumEnumKeyDecoder[E <: EnumEntry](implicit
    e: enumeratum.Enum[E]
  ): FdbKeyDeserializer[E] = { (in: FdbTupleReader) =>
    Try(e.withName(in.getString)) match {
      case Failure(e) => Left(GenericKeyDeserializationException(e.getMessage, e))
      case Success(value) => Right(value)
    }
  }

  implicit def refinedFdbKeyDeserializer[F[_, _], T, P](implicit
    deserializer: FdbKeyDeserializer[T],
    refType: RefType[F],
    validate: Validate[T, P]
  ): FdbKeyDeserializer[F[T, P]] = (in: FdbTupleReader) => {
    import cats.syntax.either._
    deserializer.deserialize(in).flatMap { value =>
      refType.refine[P](value).leftMap(GenericKeyDeserializationException(_))
    }
  }

  implicit def optionFdbKeyDecoder[T](implicit
    decoder: FdbKeyDeserializer[T]
  ): FdbKeyDeserializer[Option[T]] = { in: FdbTupleReader =>
    for {
      nonEmpty <- booleanFdbKeyDecoder.deserialize(in)
      ret <- if (nonEmpty) decoder.deserialize(in).map(v => Some(v)) else Right(None)
    } yield ret
  }

  implicit def gen[T]: FdbKeyDeserializer[T] = macro Magnolia.gen[T]

  def combine[A](ctx: CaseClass[FdbKeyDeserializer, A]): FdbKeyDeserializer[A] = (in: FdbTupleReader) => {
    ctx.constructMonadic { param => param.typeclass.deserialize(in) }
  }

  def dispatch[A, Tag](ctx: SealedTrait[FdbKeyDeserializer, A])(implicit
    tag: FdbKeyCoproductTag.Aux[A, Tag],
    tagDeserializer: FdbKeyDeserializer[Tag]
  ): FdbKeyDeserializer[A] =
    (in: FdbTupleReader) => {
      for {
        tagFromTuple <- tagDeserializer.deserialize(in)
        result <- {
          tag.tagToSubType(ctx.subtypes, tagFromTuple) match {
            case Some(subType) =>
              subType.typeclass.deserialize(in)
            case None =>
              Left(GenericKeyDeserializationException(
                s"Cannot determine subType for ${ctx.typeName} from tag $tagFromTuple"
              ))
          }
        }
      } yield result
    }
}
