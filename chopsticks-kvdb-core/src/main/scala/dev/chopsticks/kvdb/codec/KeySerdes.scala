package dev.chopsticks.kvdb.codec

import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult
import shapeless._
import shapeless.ops.hlist.FlatMapper
import dev.chopsticks.kvdb.util.UnusedImplicits._

trait KeySerdes[P] extends KeySerializer[P] with KeyDeserializer[P] {
  type Flattened <: HList

  def describe: String
}

trait DerivedKeySerdes[P] extends KeySerdes[P]

object DerivedKeySerdes extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList, C <: KeyCodec] = DerivedKeySerdes[P] {
    type Flattened = F
    type Codec = C
  }
  // scalastyle:on

  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T]: Case.Aux[T, T :: HNil] = at[T](v => v :: HNil)
  }

  object flatten extends LowPriorityFlatten {
    implicit def hlistCase[L <: HList, O <: HList](
      implicit lfm: Lazy[FlatMapper.Aux[flatten.type, L, O]]
    ): Case.Aux[L, O] = at[L](lfm.value(_))

    implicit def instanceCase[C <: Product, H <: HList, O <: HList](
      implicit gen: Lazy[Generic.Aux[C, H]],
      lfm: Lazy[FlatMapper.Aux[flatten.type, H, O]]
    ): Case.Aux[C, O] = {
      at[C](c => lfm.value(gen.value.to(c)))
    }
  }

  implicit def deriveProduct[P <: Product, R <: HList, F <: HList, C <: KeyCodec](
    implicit
    g: Generic.Aux[P, R],
    f: FlatMapper.Aux[flatten.type, R, F],
    serializer: Lazy[KeySerializer.Aux[P, C]],
    deserializer: Lazy[KeyDeserializer[P]],
    typ: Typeable[P]
  ): Aux[P, F, C] = {
    g.unused()
    f.unused()
    logger.debug(s"[DerivedDbKey][deriveProduct] ${typ.describe}")

    new DerivedKeySerdes[P] {
      type Flattened = F
      type Codec = C

      def describe: String = typ.describe

      def deserialize(bytes: Array[Byte]): KeyDeserializationResult[P] = {
        deserializer.value.deserialize(bytes)
      }

      def serialize(value: P): Array[Byte] = {
        serializer.value.serialize(value)
      }
    }
  }

  implicit def deriveAny[V, C <: KeyCodec](
    implicit
    serializer: Lazy[KeySerializer.Aux[V, C]],
    deserializer: Lazy[KeyDeserializer[V]],
    typ: Typeable[V],
    ev: V <:!< Product
  ): Aux[V, V :: HNil, C] = {
    ev.unused()
    logger.debug(s"[DerivedDbKey][deriveAny] ${typ.describe}")

    new DerivedKeySerdes[V] {
      type Flattened = V :: HNil
      type Codec = C

      def describe: String = typ.describe

      def deserialize(bytes: Array[Byte]): KeyDeserializationResult[V] = {
        deserializer.value.deserialize(bytes)
      }

      def serialize(value: V): Array[Byte] = {
        serializer.value.serialize(value)
      }
    }
  }
}

object KeySerdes extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList, C <: KeyCodec] = KeySerdes[P] {
    type Flattened = F
    type Codec = C
  }
  // scalastyle:on

  def apply[A](implicit d: DerivedKeySerdes[A]): Aux[A, d.Flattened, d.Codec] =
    d.asInstanceOf[Aux[A, d.Flattened, d.Codec]]

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
      if (b1 != b2) return (b1 & 0xff) - (b2 & 0xff)
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
