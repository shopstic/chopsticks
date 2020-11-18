package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.ValueDeserializer.{GenericValueDeserializationException, ValueDeserializationResult}
import cats.syntax.either._
import dev.chopsticks.kvdb.codec.compressed.{CompressionDescription, SnappyCompressed}
import org.xerial.snappy.{Snappy, SnappyCodec}

trait ValueSerdes[T] extends ValueSerializer[T] with ValueDeserializer[T]

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

  implicit def snappyCompressedValueSerdes[A: ValueSerdes: CompressionDescription]: ValueSerdes[SnappyCompressed[A]] = {
    new ValueSerdes[SnappyCompressed[A]] {
      override def deserialize(bytes: Array[Byte]): ValueDeserializationResult[SnappyCompressed[A]] = {
        val raw = if (SnappyCodec.hasMagicHeaderPrefix(bytes)) Snappy.uncompress(bytes) else bytes
        ValueSerdes.deserialize[A](raw).map(new SnappyCompressed(_))
      }

      override def serialize(value: SnappyCompressed[A]): Array[Byte] = {
        val serializedValue = ValueSerdes.serialize(value.value)
        val minCompressionBlockSize = implicitly[CompressionDescription[A]].minCompressionBlockSize
        if (serializedValue.length >= minCompressionBlockSize) Snappy.compress(serializedValue) else serializedValue
      }
    }
  }
}
