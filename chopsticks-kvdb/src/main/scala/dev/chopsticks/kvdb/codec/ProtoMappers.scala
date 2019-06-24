package dev.chopsticks.kvdb.codec

import java.nio.ByteBuffer
import java.time.{Duration => _, _}
import java.util.UUID

import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import com.google.protobuf.wrappers.{BytesValue, StringValue, UInt32Value}
import enumeratum.{Enum, EnumEntry}

import scala.concurrent.duration._
import scalapb.TypeMapper

object ProtoMappers {
  private[codec] def combine[A, B, C](implicit m1: TypeMapper[A, B], m2: TypeMapper[B, C]): TypeMapper[A, C] = {
    TypeMapper[A, C](m1.toCustom _ andThen m2.toCustom)(m2.toBase _ andThen m1.toBase)
  }

  private[codec] def encodeNonCompactBigDecimal(
    unscaledValueByteArray: Array[Byte],
    unscaledValueLength: Int,
    scale: Int
  ): ByteString = {
    val buffer = ByteBuffer
      .allocate(unscaledValueLength + 9)
      .put(0.toByte)
      .putInt(unscaledValueLength)
      .put(unscaledValueByteArray)
      .putInt(scale)

    ByteString.copyFrom(buffer.array())
  }

  private[codec] def encodeCompactBigDecimal(
    unscaledValueByteArray: Array[Byte],
    unscaledValueLength: Int,
    scale: Int
  ): ByteString = {
    val buffer = ByteBuffer
      .allocate(unscaledValueLength + 3)
      .put(1.toByte)
      .put(unscaledValueLength.toByte)
      .put(unscaledValueByteArray)
      .put(scale.toByte)
    ByteString.copyFrom(buffer.array())
  }

  private[codec] def encodeBigDecimal(v: BigDecimal): ByteString = {
    val unscaledValue = v.underlying().unscaledValue()
    val unscaledValueByteArray = unscaledValue.toByteArray
    val unscaledValueLength = unscaledValueByteArray.length
    val scale = v.scale
    val isCompact = unscaledValueLength <= Byte.MaxValue && scale <= Byte.MaxValue

    if (isCompact) {
      encodeCompactBigDecimal(unscaledValueByteArray, unscaledValueLength, scale)
    }
    else {
      encodeNonCompactBigDecimal(unscaledValueByteArray, unscaledValueLength, scale)
    }
  }

  private[codec] def decodeNonCompactBigDecimal(buffer: ByteBuffer): BigDecimal = {
    val unscaledValueLength = buffer.getInt()
    val unscaledValueByteArray = new Array[Byte](unscaledValueLength)
    val _ = buffer.get(unscaledValueByteArray, 0, unscaledValueLength)
    val scale = buffer.getInt()
    val unscaledValue = BigInt(unscaledValueByteArray)
    BigDecimal(unscaledValue, scale)
  }

  private[codec] def decodeCompactBigDecimal(buffer: ByteBuffer): BigDecimal = {
    val unscaledValueLength = buffer.get().toInt
    val unscaledValueByteArray = new Array[Byte](unscaledValueLength)
    val _ = buffer.get(unscaledValueByteArray, 0, unscaledValueLength)
    val scale = buffer.get().toInt
    val unscaledValue = BigInt(unscaledValueByteArray)
    BigDecimal(unscaledValue, scale)
  }

  private[codec] val zeroBigDecimal = BigDecimal(0)
  private[codec] def decodeBigDecimal(bytes: ByteString): BigDecimal = {
    if (bytes.isEmpty) zeroBigDecimal
    else {
      val buffer = bytes.asReadOnlyByteBuffer()
      val isCompact = buffer.get()

      if (isCompact == 1) {
        decodeCompactBigDecimal(buffer)
      }
      else {
        decodeNonCompactBigDecimal(buffer)
      }
    }
  }

  implicit val intLongMapper: TypeMapper[Int, Long] = TypeMapper[Int, Long](_.toLong)(Math.toIntExact)
  implicit val durationLongMapper: TypeMapper[Long, FiniteDuration] = TypeMapper(Duration.fromNanos)(_.toNanos)
  implicit val localDateLongMapper: TypeMapper[Long, LocalDate] = TypeMapper(LocalDate.ofEpochDay)(_.toEpochDay)
  implicit val localDateIntMapper: TypeMapper[Int, LocalDate] = combine[Int, Long, LocalDate]

  implicit val localDateUin32ValueMapper: TypeMapper[UInt32Value, LocalDate] = combine[UInt32Value, Int, LocalDate]

  implicit val instantTimestampMapper: TypeMapper[Timestamp, Instant] = TypeMapper { t: Timestamp =>
    Instant.ofEpochSecond(t.seconds, t.nanos.toLong)
  } { t: Instant =>
    Timestamp(t.getEpochSecond, t.getNano)
  }

  implicit val localTimeMapper: TypeMapper[Long, LocalTime] = TypeMapper(LocalTime.ofNanoOfDay)(_.toNanoOfDay)
  implicit val yearMonthLongMapper: TypeMapper[Long, YearMonth] = TypeMapper((v: Long) => {
    if (v == 0) YearMonth.of(0, 1)
    else {
      val year = v / 100
      val month = v - year * 100
      YearMonth.of(year.toInt, month.toInt)
    }
  })((v: YearMonth) => {
    v.getYear.toLong * 100 + v.getMonthValue
  })

  implicit val yearMonthIntMapper: TypeMapper[Int, YearMonth] = combine[Int, Long, YearMonth]

  implicit val bigDecimalStringMapper: TypeMapper[String, BigDecimal] = TypeMapper((v: String) => {
    if (v.length == 0) BigDecimal(0) else BigDecimal(v)
  })(_.toString)
  implicit val bigDecimalStringValueMapper: TypeMapper[StringValue, BigDecimal] =
    combine[StringValue, String, BigDecimal]
  implicit val bigDecimalByteStringMapper: TypeMapper[ByteString, BigDecimal] =
    TypeMapper(decodeBigDecimal)(encodeBigDecimal)
  implicit val bigDecimalBytesValueMapper: TypeMapper[BytesValue, BigDecimal] =
    combine[BytesValue, ByteString, BigDecimal]

  implicit val uuidMapper: TypeMapper[String, UUID] = TypeMapper((uuid: String) => {
    if (uuid.nonEmpty) UUID.fromString(uuid) else UUID.randomUUID()
  })(_.toString)

  implicit val dayOfWeekMapper: TypeMapper[Int, DayOfWeek] = TypeMapper(DayOfWeek.of)(_.getValue)

  implicit def enumeratumTypeMapper[E <: EnumEntry](implicit e: Enum[E]): TypeMapper[String, E] =
    TypeMapper(e.withName)(_.entryName)
}
