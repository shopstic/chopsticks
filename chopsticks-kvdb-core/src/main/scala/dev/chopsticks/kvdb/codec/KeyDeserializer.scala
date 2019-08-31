package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.KeyDeserializer.DbKeyDecodeResult

import scala.util.control.NoStackTrace

trait KeyDeserializer[T] {
  def decode(bytes: Array[Byte]): DbKeyDecodeResult[T]
}

object KeyDeserializer {
  sealed trait DecodingFailure extends NoStackTrace

  // scalastyle:off null
  final case class GenericDecodingException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)
      with DecodingFailure

  final case class WrongInputSizeException(message: String, targetName: String)
      extends RuntimeException(message)
      with DecodingFailure

  type DbKeyDecodeResult[T] = Either[DecodingFailure, T]

}
