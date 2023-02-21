package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult

import scala.deriving.*

trait KeySerdes[K] extends KeySerializer[K] with KeyDeserializer[K] with KeyPrefixSerializer[K] with KeyFlattening[K]

object KeySerdes extends LowPriorityImplicits:
  type Aux[P, F <: Tuple] = KeySerdes[P] {
    type Flattened = F
  }

  given derived[K: Mirror.ProductOf](using K <:< Product)(using
    flattening: KeyFlattening[K],
    serializer: KeySerializer[K],
    prefixSerializer: KeyPrefixSerializer[K],
    deserializer: KeyDeserializer[K]
  ): KeySerdes[K] =
    new KeySerdes[K]:
      type Flattened = flattening.Flattened
      override def flatten(value: K): Flattened =
        flattening.flatten(value)
      override def serialize(key: K): Array[Byte] =
        serializer.serialize(key)
      override def serializePrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]) =
        prefixSerializer.serializePrefix(prefix)
      override def serializeLiteralPrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]) =
        prefixSerializer.serializeLiteralPrefix(prefix)
      override def deserialize(bytes: Array[Byte]): KeyDeserializationResult[K] =
        deserializer.deserialize(bytes)

  def ordering[A](using dbKey: KeySerdes[A]): Ordering[A] =
    (x: A, y: A) => KeySerdes.compare(dbKey.serialize(x), dbKey.serialize(y))

  def deserialize[V](bytes: Array[Byte])(using deserializer: KeySerdes[V]): KeyDeserializationResult[V] =
    deserializer.deserialize(bytes)

  def serialize[V](value: V)(using serializer: KeySerdes[V]): Array[Byte] =
    serializer.serialize(value)

  def compare(a1: Array[Byte], a2: Array[Byte]): Int =
    val minLen = Math.min(a1.length, a2.length)
    var i = 0
    while (i < minLen) {
      val b1 = a1(i)
      val b2 = a2(i)
      //noinspection ScalaStyle
      //scalastyle:off
      if (b1 != b2) return (b1 & 0xFF) - (b2 & 0xFF)
      //scalastyle:on
      i += 1
    }
    a1.length - a2.length

  def isEqual(o1: Array[Byte], o2: Array[Byte]): Boolean =
    val len = o1.length
    len == o2.length && {
      var i = 0
      while (i < len && o1(i) == o2(i)) i += 1
      i == len
    }

  def isPrefix(prefix: Array[Byte], a: Array[Byte]): Boolean =
    var i = 0
    val len = prefix.length
    while (i < len && prefix(i) == a(i)) i += 1
    i == len

end KeySerdes

trait LowPriorityImplicits:
  implicit def primitive[K](
    using
    serializer: KeySerializer[K],
    prefixSerializer: KeyPrefixSerializer[K],
    deserializer: KeyDeserializer[K]
  ): KeySerdes[K] =
    new KeySerdes[K]:
      override type Flattened = K *: EmptyTuple

      override def flatten(value: K): Flattened = value *: EmptyTuple

      override def serialize(key: K): Array[Byte] = serializer.serialize(key)

      override def serializePrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]) =
        prefixSerializer.serializePrefix(prefix)

      override def serializeLiteralPrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, K]): Array[Byte] =
        prefixSerializer.serializeLiteralPrefix(prefix)

      override def deserialize(bytes: Array[Byte]): KeyDeserializationResult[K] =
        deserializer.deserialize(bytes)
