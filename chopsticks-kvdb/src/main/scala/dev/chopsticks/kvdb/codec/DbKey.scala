package dev.chopsticks.kvdb.codec

import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.codec.DbKeyDecoder.DbKeyDecodeResult
import shapeless._
import shapeless.ops.hlist.FlatMapper

trait DbKey[P] extends DbKeyEncoder[P] with DbKeyDecoder[P] {
  type Flattened <: HList

  def describe: String
}

trait DerivedDbKey[P] extends DbKey[P]

object DerivedDbKey extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList, C] = DerivedDbKey[P] {
    type Flattened = F
    type Codec = C
  }
  // scalastyle:on

  implicit final class UnusedOps[A](private val a: A) extends AnyVal {
    def unused(): Unit = ()
  }

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
    encoder: Lazy[DbKeyEncoder.Aux[P, C]],
    decoder: Lazy[DbKeyDecoder[P]],
    typ: Typeable[P]
  ): Aux[P, F, C] = {
    g.unused()
    f.unused()
    logger.debug(s"[DerivedDbKey][deriveInstance] ${typ.describe}")

    new DerivedDbKey[P] {
      type Flattened = F
      type Codec = C

      def describe: String = typ.describe

      def decode(bytes: Array[Byte]): DbKeyDecodeResult[P] = {
        decoder.value.decode(bytes)
      }

      def encode(value: P): Array[Byte] = {
        encoder.value.encode(value)
      }
    }
  }
}

object DbKey extends StrictLogging {
  //noinspection ScalaStyle
  // scalastyle:off
  type Aux[P, F <: HList, C] = DbKey[P] {
    type Flattened = F
    type Codec = C
  }
  // scalastyle:on

  def apply[A](implicit d: DerivedDbKey[A]): Aux[A, d.Flattened, d.Codec] = d.asInstanceOf[Aux[A, d.Flattened, d.Codec]]

  def ordering[A](implicit dbKey: DbKey[A]): Ordering[A] =
    (x: A, y: A) => DbKey.compare(dbKey.encode(x), dbKey.encode(y))

  def deriveGeneric[V, C](
    implicit
    encoder: Lazy[DbKeyEncoder.Aux[V, C]],
    decoder: Lazy[DbKeyDecoder[V]],
    typ: Typeable[V]
  ): Aux[V, V :: HNil, C] = {
    new DbKey[V] {
      type Flattened = V :: HNil
      type Codec = C

      def describe: String = typ.describe

      def decode(bytes: Array[Byte]): DbKeyDecodeResult[V] = {
        decoder.value.decode(bytes)
      }

      def encode(value: V): Array[Byte] = {
        encoder.value.encode(value)
      }
    }
  }

  def decode[V](bytes: Array[Byte])(implicit decoder: DbKey[V]): DbKeyDecodeResult[V] = {
    decoder.decode(bytes)
  }

  def encode[V](value: V)(implicit encoder: DbKey[V]): Array[Byte] = encoder.encode(value)

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
