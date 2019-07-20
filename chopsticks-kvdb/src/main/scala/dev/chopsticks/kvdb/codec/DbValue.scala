package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.DbValueDecoder.DbValueDecodeResult

trait DbValue[T] extends DbValueEncoder[T] with DbValueDecoder[T]

object DbValue {
  def apply[T](implicit f: DbValue[T]): DbValue[T] = f

  def create[T](encoder: T => Array[Byte], decoder: Array[Byte] => DbValueDecodeResult[T]): DbValue[T] = {
    new DbValue[T] {
      def decode(bytes: Array[Byte]): DbValueDecodeResult[T] = decoder(bytes)

      def encode(value: T): Array[Byte] = encoder(value)
    }
  }

  def encode[T](value: T)(implicit encoder: DbValueEncoder[T]): Array[Byte] = {
    encoder.encode(value)
  }

  def decode[T](bytes: Array[Byte])(implicit decoder: DbValueDecoder[T]): DbValueDecodeResult[T] = {
    decoder.decode(bytes)
  }
}
