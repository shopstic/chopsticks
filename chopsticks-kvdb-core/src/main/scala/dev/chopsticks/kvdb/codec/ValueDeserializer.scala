package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.ValueDeserializationResult

import scala.util.control.NoStackTrace

trait ValueDeserializer[T] {
  def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T]
}

object ValueDeserializer {
  trait ValueDeserializationFailure extends NoStackTrace

  // scalastyle:off null
  final case class GenericValueDeserializationException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)
      with ValueDeserializationFailure

  type ValueDeserializationResult[T] = Either[ValueDeserializationFailure, T]
}
