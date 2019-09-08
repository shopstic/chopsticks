package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.ValueDeserializationResult

trait ValueSerdes[T] extends ValueSerializer[T] with ValueDeserializer[T]

object ValueSerdes {
  def apply[T](implicit f: ValueSerdes[T]): ValueSerdes[T] = f

  def create[T](
    serializer: T => Array[Byte],
    deserializer: Array[Byte] => ValueDeserializationResult[T]
  ): ValueSerdes[T] = {
    new ValueSerdes[T] {
      def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = deserializer(bytes)

      def serialize(value: T): Array[Byte] = serializer(value)
    }
  }

  def serialize[T](value: T)(implicit encoder: ValueSerializer[T]): Array[Byte] = {
    encoder.serialize(value)
  }

  def deserialize[T](bytes: Array[Byte])(implicit decoder: ValueDeserializer[T]): ValueDeserializationResult[T] = {
    decoder.deserialize(bytes)
  }

  implicit val byteArrayValueSerdes: ValueSerdes[Array[Byte]] = ValueSerdes.create[Array[Byte]](identity, bytes => Right(bytes))
  implicit val unitValueSerdes: ValueSerdes[Unit] = ValueSerdes.create[Unit](_ => Array.emptyByteArray, _ => Right(()))
}
