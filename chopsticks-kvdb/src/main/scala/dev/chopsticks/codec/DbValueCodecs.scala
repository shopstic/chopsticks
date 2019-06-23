package dev.chopsticks.codec

import scala.util.control.NoStackTrace

object DbValueCodecs {
  sealed trait DbValueDecodingFailure extends NoStackTrace

  // scalastyle:off null
  final case class GenericDecodingException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)
      with DbValueDecodingFailure

  type DbValueDecodeResult[T] = Either[DbValueDecodingFailure, T]

  trait ToDbValue[T] {
    def encode(value: T): Array[Byte]
  }

  trait FromDbValue[T] {
    def decode(bytes: Array[Byte]): DbValueDecodeResult[T]
  }
}
