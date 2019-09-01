package dev.chopsticks.kvdb.codec.protobuf_value

import dev.chopsticks.kvdb.codec.ValueDeserializer.{GenericValueDeserializationException, ValueDeserializationResult}
import dev.chopsticks.kvdb.codec.ValueSerdes
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}

import scala.util.control.NonFatal

final class ProtobufEnumValueSerdes[T <: GeneratedEnum](implicit comp: GeneratedEnumCompanion[T])
    extends ValueSerdes[T] {
  def serialize(value: T): Array[Byte] = Array(value.value.toByte)

  def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = {
    try {
      Right(comp.fromValue(bytes(0).toInt))
    } catch {
      case NonFatal(e) =>
        Left(
          GenericValueDeserializationException(s"Failed decoding to ${comp.scalaDescriptor.fullName}: ${e.toString}", e)
        )
    }
  }
}
