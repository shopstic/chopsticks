package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.ValueDeserializer.{GenericValueDeserializationException, ValueDeserializationResult}
import dev.chopsticks.kvdb.codec.ValueSerdes
import scalapb.{GeneratedMessageCompanion, GeneratedSealedOneof, TypeMapper}

import scala.util.control.NonFatal

final class ProtobufSealedOneofValueSerdes[T <: GeneratedSealedOneof, M <: T#MessageType](implicit
  typeMapper: TypeMapper[M, T],
  comp: GeneratedMessageCompanion[M]
) extends ValueSerdes[T] {
  def serialize(value: T): Array[Byte] = value.asMessage.toByteArray

  def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = {
    try {
      Right(typeMapper.toCustom(comp.parseFrom(bytes)))
    }
    catch {
      case NonFatal(e) =>
        Left(
          GenericValueDeserializationException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e)
        )
    }
  }
}
