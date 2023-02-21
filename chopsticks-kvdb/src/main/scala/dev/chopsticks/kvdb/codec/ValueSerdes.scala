package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.{ValueDeserializationFailure, ValueDeserializationResult}
import cats.syntax.either._

trait ValueSerdes[T] extends ValueSerializer[T] with ValueDeserializer[T]

object ValueSerdes:
  def apply[T](using serdes: ValueSerdes[T]): ValueSerdes[T] = serdes

  def fromKeySerdes[T](using keySerdes: KeySerdes[T]): ValueSerdes[T] =
    create[T](
      value => keySerdes.serialize(value),
      bytes =>
        keySerdes.deserialize(bytes).leftMap { e =>
          ValueDeserializationFailure.GenericValueDeserializationException(e.getMessage, e.getCause)
        }
    )

  def create[T](
    serializer: T => Array[Byte],
    deserializer: Array[Byte] => ValueDeserializationResult[T]
  ): ValueSerdes[T] =
    new ValueSerdes[T]:
      def deserialize(bytes: Array[Byte]): ValueDeserializationResult[T] = deserializer(bytes)
      def serialize(value: T): Array[Byte] = serializer(value)

  def serialize[T](value: T)(using encoder: ValueSerializer[T]): Array[Byte] =
    encoder.serialize(value)

  def deserialize[T](bytes: Array[Byte])(using decoder: ValueDeserializer[T]): ValueDeserializationResult[T] =
    decoder.deserialize(bytes)

  implicit val byteArrayValueSerdes: ValueSerdes[Array[Byte]] =
    ValueSerdes.create[Array[Byte]](identity, bytes => Right(bytes))

  implicit val unitValueSerdes: ValueSerdes[Unit] =
    ValueSerdes.create[Unit](_ => Array.emptyByteArray, _ => Right(()))
