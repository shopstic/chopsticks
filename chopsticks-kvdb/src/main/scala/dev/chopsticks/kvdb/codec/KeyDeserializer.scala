package dev.chopsticks.kvdb.codec

import scala.util.control.NoStackTrace

trait KeyDeserializer[T]:
  def deserialize(bytes: Array[Byte]): KeyDeserializer.KeyDeserializationResult[T]

object KeyDeserializer:
  enum DecodingFailure(message: String, cause: Throwable) extends RuntimeException(message, cause) with NoStackTrace:
    case GenericKeyDeserializationException(message: String, cause: Throwable = null)
        extends DecodingFailure(message, cause)
    case WrongInputSizeException(message: String) extends DecodingFailure(message, null)

  type KeyDeserializationResult[T] = Either[DecodingFailure, T]
