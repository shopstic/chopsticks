package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.ValueDeserializationResult

trait ValueSerdes[T] extends ValueSerializer[T] with ValueDeserializer[T]

object ValueSerdes {
  def apply[T](implicit f: ValueSerdes[T]): ValueSerdes[T] = f

  def create[T](encoder: T => Array[Byte], decoder: Array[Byte] => ValueDeserializationResult[T]): ValueSerdes[T] = {
    new ValueSerdes[T] {
      def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = decoder(bytes)

      def serialize(value: T): Array[Byte] = encoder(value)
    }
  }

  def encode[T](value: T)(implicit encoder: ValueSerializer[T]): Array[Byte] = {
    encoder.serialize(value)
  }

  def decode[T](bytes: Array[Byte])(implicit decoder: ValueDeserializer[T]): ValueDeserializationResult[T] = {
    decoder.deserialize(bytes)
  }
}
