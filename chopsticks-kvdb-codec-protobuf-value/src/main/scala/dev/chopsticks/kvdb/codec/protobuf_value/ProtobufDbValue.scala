package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.DbValue
import dev.chopsticks.kvdb.codec.DbValueDecoder.{DbValueDecodeResult, DbValueGenericDecodingException}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.util.control.NonFatal

final class ProtobufDbValue[T <: GeneratedMessage with Message[T]](implicit comp: GeneratedMessageCompanion[T])
    extends DbValue[T] {
  def encode(value: T): Array[Byte] = value.toByteArray

  def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = {
    try Right(comp.parseFrom(bytes))
    catch {
      case NonFatal(e) =>
        Left(
          DbValueGenericDecodingException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e)
        )
    }
  }
}
