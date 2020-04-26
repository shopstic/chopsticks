package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.ValueDeserializer.{GenericValueDeserializationException, ValueDeserializationResult}
import dev.chopsticks.kvdb.codec.ValueSerdes
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.util.control.NonFatal

final class ProtobufValueSerdes[T <: GeneratedMessage](implicit comp: GeneratedMessageCompanion[T])
    extends ValueSerdes[T] {
  def serialize(value: T): Array[Byte] = value.toByteArray

  def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = {
    try {
      Right(comp.parseFrom(bytes))
    }
    catch {
      case NonFatal(e) =>
        Left(
          GenericValueDeserializationException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e)
        )
    }
  }
}
