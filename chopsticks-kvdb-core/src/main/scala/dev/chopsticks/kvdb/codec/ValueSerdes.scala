package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.{GenericValueDeserializationException, ValueDeserializationResult}
import cats.syntax.either._

trait ValueSerdes[T] extends ValueSerializer[T] with ValueDeserializer[T] {
  def bimap[A](to: T => A)(from: A => T): ValueSerdes[A] = {
    val that = this
    new ValueSerdes[A] {
      override def deserialize(bytes: Array[Byte]): ValueDeserializationResult[A] = that.deserialize(bytes).map(to)
      override def serialize(value: A): Array[Byte] = that.serialize(from(value))
    }
  }
}

object ValueSerdes {
  def apply[T](implicit f: ValueSerdes[T]): ValueSerdes[T] = f

  def fromKeySerdes[T](implicit keySerdes: KeySerdes[T]): ValueSerdes[T] = {
    create[T](
      value => keySerdes.serialize(value),
      bytes =>
        keySerdes.deserialize(bytes).leftMap { e =>
          GenericValueDeserializationException(e.getMessage, e.getCause)
        }
    )
  }

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

  implicit val byteArrayValueSerdes: ValueSerdes[Array[Byte]] =
    ValueSerdes.create[Array[Byte]](identity, bytes => Right(bytes))

  implicit val unitValueSerdes: ValueSerdes[Unit] = ValueSerdes.create[Unit](_ => Array.emptyByteArray, _ => Right(()))
}
