package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.DbValue
import dev.chopsticks.kvdb.codec.DbValueDecoder.{DbValueDecodeResult, DbValueGenericDecodingException}
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}

import scala.util.control.NonFatal

final class ProtobufEnumDbValue[T <: GeneratedEnum](implicit comp: GeneratedEnumCompanion[T]) extends DbValue[T] {
  def encode(value: T): Array[Byte] = Array(value.value.toByte)

  def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = {
    try Right(comp.fromValue(bytes(0).toInt))
    catch {
      case NonFatal(e) =>
        Left(
          DbValueGenericDecodingException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e)
        )
    }
  }
}
