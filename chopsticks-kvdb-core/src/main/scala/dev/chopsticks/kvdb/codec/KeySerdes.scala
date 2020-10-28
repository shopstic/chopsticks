package dev.chopsticks.kvdb.codec

import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult

import scala.annotation.nowarn
import shapeless.{::, <:!<, HList, HNil, Typeable}

trait KeySerdes[K] extends KeySerializer[K] with KeyDeserializer[K] with KeyPrefixSerializer[K] with KeyFlattening[K] {
  def describe: String
}

object KeySerdes extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList] = KeySerdes[P] {
    type Flattened = F
  }
  // scalastyle:on

  def apply[T](implicit s: KeySerdes[T]): Aux[T, s.Flattened] = s

  implicit def deriveProduct[P <: Product, F <: HList](implicit
    flattening: KeyFlattening.Aux[P, F],
    serializer: KeySerializer[P],
    prefixSerializer: KeyPrefixSerializer[P],
    deserializer: KeyDeserializer[P],
    typ: Typeable[P]
  ): Aux[P, F] = {
    logger.debug(s"[DerivedDbKey][deriveProduct] ${typ.describe}")

    new KeySerdes[P] {
      type Flattened = F

      override def flatten(value: P): Flattened = flattening.flatten(value)

      override def describe: String = typ.describe

      override def deserialize(bytes: Array[Byte]): KeyDeserializationResult[P] = {
        deserializer.deserialize(bytes)
      }

      override def serialize(value: P): Array[Byte] = {
        serializer.serialize(value)
      }

      override def serializePrefix[V](prefix: V)(implicit ev: KeyPrefix[V, P]): Array[Byte] =
        prefixSerializer.serializePrefix(prefix)
    }
  }

  implicit def deriveAny[V](implicit
    @nowarn ev: V <:!< Product,
    serializer: KeySerializer[V],
    prefixSerializer: KeyPrefixSerializer[V],
    deserializer: KeyDeserializer[V],
    typ: Typeable[V]
  ): Aux[V, V :: HNil] = {
    logger.debug(s"[DerivedDbKey][deriveAny] ${typ.describe}")

    new KeySerdes[V] {
      type Flattened = V :: HNil

      override def flatten(value: V): Flattened = value :: HNil

      override def describe: String = typ.describe

      override def deserialize(bytes: Array[Byte]): KeyDeserializationResult[V] = {
        deserializer.deserialize(bytes)
      }

      override def serialize(value: V): Array[Byte] = {
        serializer.serialize(value)
      }

      override def serializePrefix[P](prefix: P)(implicit ev: KeyPrefix[P, V]): Array[Byte] =
        prefixSerializer.serializePrefix(prefix)
    }
  }

  def ordering[A](implicit dbKey: KeySerdes[A]): Ordering[A] =
    (x: A, y: A) => KeySerdes.compare(dbKey.serialize(x), dbKey.serialize(y))

  def deserialize[V](bytes: Array[Byte])(implicit deserializer: KeySerdes[V]): KeyDeserializationResult[V] = {
    deserializer.deserialize(bytes)
  }

  def serialize[V](value: V)(implicit serializer: KeySerdes[V]): Array[Byte] = serializer.serialize(value)

  def compare(a1: Array[Byte], a2: Array[Byte]): Int = {
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
  }

  def isEqual(o1: Array[Byte], o2: Array[Byte]): Boolean = {
    val len = o1.length
    len == o2.length && {
      var i = 0
      while (i < len && o1(i) == o2(i)) i += 1
      i == len
    }
  }

  def isPrefix(prefix: Array[Byte], a: Array[Byte]): Boolean = {
    var i = 0
    val len = prefix.length
    while (i < len && prefix(i) == a(i)) i += 1
    i == len
  }
}
