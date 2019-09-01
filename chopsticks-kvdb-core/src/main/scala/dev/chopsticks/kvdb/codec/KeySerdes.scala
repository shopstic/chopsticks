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
  type Aux[P, F <: HList, C] = DerivedKeySerdes[P] {
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

  implicit def deriveInstance[P <: Product, R <: HList, F <: HList, C](
    implicit
    g: Generic.Aux[P, R],
    f: FlatMapper.Aux[flatten.type, R, F],
    encoder: Lazy[KeySerializer.Aux[P, C]],
    decoder: Lazy[KeyDeserializer[P]],
    typ: Typeable[P]
  ): Aux[P, F, C] = {
    g.unused()
    f.unused()
    logger.debug(s"[DerivedDbKey][deriveInstance] ${typ.describe}")

    new DerivedKeySerdes[P] {
      type Flattened = F
      type Codec = C

      def describe: String = typ.describe

      def decode(bytes: Array[Byte]): KeyDeserializationResult[P] = {
        decoder.value.decode(bytes)
      }

      def encode(value: P): Array[Byte] = {
        encoder.value.encode(value)
      }
    }
  }
}

object KeySerdes extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList, C] = KeySerdes[P] {
    type Flattened = F
    type Codec = C
  }
  // scalastyle:on

  def apply[A](implicit d: DerivedKeySerdes[A]): Aux[A, d.Flattened, d.Codec] =
    d.asInstanceOf[Aux[A, d.Flattened, d.Codec]]

  def ordering[A](implicit dbKey: KeySerdes[A]): Ordering[A] =
    (x: A, y: A) => KeySerdes.compare(dbKey.encode(x), dbKey.encode(y))

  def deriveGeneric[V, C](
    implicit
    encoder: Lazy[KeySerializer.Aux[V, C]],
    decoder: Lazy[KeyDeserializer[V]],
    typ: Typeable[V]
  ): Aux[V, V :: HNil, C] = {
    new KeySerdes[V] {
      type Flattened = V :: HNil
      type Codec = C

      def describe: String = typ.describe

      def decode(bytes: Array[Byte]): KeyDeserializationResult[V] = {
        decoder.value.decode(bytes)
      }

      def encode(value: V): Array[Byte] = {
        encoder.value.encode(value)
      }
    }
  }

  def decode[V](bytes: Array[Byte])(implicit decoder: KeySerdes[V]): KeyDeserializationResult[V] = {
    decoder.decode(bytes)
  }

  def encode[V](value: V)(implicit encoder: KeySerdes[V]): Array[Byte] = encoder.encode(value)

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
