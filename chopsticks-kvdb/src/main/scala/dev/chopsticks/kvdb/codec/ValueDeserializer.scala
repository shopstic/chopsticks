package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.ValueDeserializationResult

import scala.util.control.NoStackTrace

trait ValueDeserializer[T]:
  def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T]

object ValueDeserializer:
  enum ValueDeserializationFailure(message: String, cause: Throwable) extends RuntimeException(message, cause)
      with NoStackTrace:
    case GenericValueDeserializationException(message: String, cause: Throwable = null)
        extends ValueDeserializationFailure(message, cause)

  type ValueDeserializationResult[T] = Either[ValueDeserializationFailure, T]
