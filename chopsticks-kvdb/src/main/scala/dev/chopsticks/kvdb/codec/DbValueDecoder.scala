package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.DbValueDecoder.DbValueDecodeResult

import scala.util.control.NoStackTrace

trait DbValueDecoder[T] {
  def decode(bytes: Array[Byte]): DbValueDecodeResult[T]
}

object DbValueDecoder {
  sealed trait DbValueDecodingFailure extends NoStackTrace

  // scalastyle:off null
  final case class DbValueGenericDecodingException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)
      with DbValueDecodingFailure

  type DbValueDecodeResult[T] = Either[DbValueDecodingFailure, T]
}
