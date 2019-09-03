package dev.chopsticks.kvdb.codec

import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.util.UnusedImplicits._
import shapeless.ops.hlist.{IsHCons, Length, Take}
import shapeless.{<:!<, Generic, HList, Nat}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot prove that ${A} is a DbKeyPrefix of ${B}.")
trait KeyPrefix[A, B] {
  def serialize(a: A): Array[Byte]
}

object KeyPrefix extends StrictLogging {
  final private class KeyPrefixWithSerializer[A, B](serializer: KeySerializer[A]) extends KeyPrefix[A, B] {
    def serialize(a: A): Array[Byte] = serializer.serialize(a)
  }

  implicit def selfKeyPrefix[A](implicit deserializer: KeySerializer[A]): KeyPrefix[A, A] =
    new KeyPrefixWithSerializer(deserializer)

  // e.g
  // T as prefix of:
  // case class Bar(one: T, two: Boolean, three: Double)
  implicit def anyToKeyPrefixOfProduct[A, B <: Product, F <: HList, T <: HList, C <: KeyCodec](
    implicit
    f: KeySerdes.Aux[B, F, C],
    t: IsHCons.Aux[F, A, T],
    serializer: KeySerializer.Aux[A, C],
    e: A <:!< Product
  ): KeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][anyToDbKeyPrefixOfProduct] ${f.describe}")
    t.unused()
    e.unused()
    new KeyPrefixWithSerializer[A, B](serializer)
  }

  implicit def productToKeyPrefix[A <: Product, B <: Product, P <: HList, F <: HList, N <: Nat, T <: HList, C <: KeyCodec](
    implicit
    g: Generic.Aux[A, P],
    l: Length.Aux[P, N],
    f: KeySerdes.Aux[B, F, C],
    t: Take.Aux[F, N, T],
    e: P =:= T,
    n: A <:!< HList,
    encoder: KeySerializer.Aux[A, C]
  ): KeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][productToDbKeyPrefix] ${f.describe}")
    //    n.unused()
    g.unused()
    l.unused()
    t.unused()
    e.unused()
    n.unused()
    new KeyPrefixWithSerializer[A, B](encoder)
  }

  // e.g
  // String :: Boolean :: HNil
  //    as prefix of:
  // case class Bar(one: String, two: Boolean, three: Double)
  implicit def hlistToKeyPrefix[A <: HList, B <: Product, F <: HList, N <: Nat, T <: HList, C <: KeyCodec](
    implicit
    l: Length.Aux[A, N],
    f: KeySerdes.Aux[B, F, C],
    t: Take.Aux[F, N, T],
    e: A =:= T,
    encoder: KeySerializer.Aux[A, C]
  ): KeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][hlistToDbKeyPrefix] ${f.describe}")
    l.unused()
    t.unused()
    e.unused()
    new KeyPrefixWithSerializer[A, B](encoder)
  }

  def apply[A, B](implicit e: KeyPrefix[A, B]): KeyPrefix[A, B] = e

//  implicit val literalStringDbKeyPrefix: DbKeyPrefix[String, String] = (a: String) =>
//    KvdbSerdesUtils.stringToByteArray(a)
}
