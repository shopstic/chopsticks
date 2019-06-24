package dev.chopsticks.kvdb.codec

import java.time.Instant

import dev.chopsticks.kvdb.codec.DbValueCodecs.{DbValueDecodeResult, FromDbValue, GenericDecodingException, ToDbValue}
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb._

import scala.util.control.NonFatal

trait DbValue[T] extends ToDbValue[T] with FromDbValue[T]

object DbValue {
  def apply[T](implicit f: DbValue[T]): DbValue[T] = f

  def create[T](encoder: T => Array[Byte], decoder: Array[Byte] => DbValueDecodeResult[T]): DbValue[T] = {
    new DbValue[T] {
      def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = decoder(bytes)

      def encode(value: T): Array[Byte] = encoder(value)
    }
  }

  def encode[T](value: T)(implicit encoder: ToDbValue[T]): Array[Byte] = {
    encoder.encode(value)
  }

  def decode[T](bytes: Array[Byte])(implicit decoder: FromDbValue[T]): DbValueDecodeResult[T] = {
    decoder.decode(bytes)
  }

  final class ProtobufDbValue[T <: GeneratedMessage with Message[T]](implicit comp: GeneratedMessageCompanion[T])
      extends DbValue[T] {
    def encode(value: T): Array[Byte] = value.toByteArray

    def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = {
      try Right(comp.parseFrom(bytes))
      catch {
        case NonFatal(e) =>
          Left(GenericDecodingException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e))
      }
    }
  }

  final class ProtobufEnumDbValue[T <: GeneratedEnum](implicit comp: GeneratedEnumCompanion[T]) extends DbValue[T] {
    def encode(value: T): Array[Byte] = Array(value.value.toByte)

    def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = {
      try Right(comp.fromValue(bytes(0).toInt))
      catch {
        case NonFatal(e) =>
          Left(GenericDecodingException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e))
      }
    }
  }

  implicit def protobufMessageDbValue[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion]: DbValue[T] =
    new ProtobufDbValue[T]
  implicit def protobufEnumDbValue[T <: GeneratedEnum: GeneratedEnumCompanion]: DbValue[T] = new ProtobufEnumDbValue[T]
  implicit def dbKeyToDbValue[T](implicit dbKey: DbKey[T]): DbValue[T] = new DbValue[T] {
    import cats.syntax.either._
    def decode(bytes: Array[Byte]): DbValueDecodeResult[T] =
      dbKey.decode(bytes).leftMap(e => GenericDecodingException(e.getMessage, e))
    def encode(value: T): Array[Byte] = dbKey.encode(value)
  }

  implicit val instantDbValue: DbValue[Instant] = create[Instant](v => {
    KvdbSerdesUtils.instantToEpochNanos(v).toByteArray
  }, v => {
    Right(KvdbSerdesUtils.epochNanosToInstant(BigInt(v)))
  })
}
