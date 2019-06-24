package dev.chopsticks.kvdb.codec

import com.sleepycat.bind.tuple.TupleOutput
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.codec.DbKeyCodecs.ToDbKey
import shapeless.ops.hlist.{IsHCons, Length, Take}
import shapeless.{Generic, HList, Nat}
import UnusedImplicits._
import dev.chopsticks.kvdb.util.KvdbSerdesUtils

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot prove that ${A} is a DbKeyPrefix of ${B}.")
trait DbKeyPrefix[A, B] {
  def encode(a: A): Array[Byte]
}

final private class DbKeyPrefixWithEncoder[A, B](encoder: ToDbKey[A]) extends DbKeyPrefix[A, B] {
  def encode(a: A): Array[Byte] = encoder.encode(new TupleOutput(), a).toByteArray
}

trait DbKeyPrefixPriority3Implicits extends StrictLogging {
  implicit def selfDbKeyPrefix[A](implicit encoder: ToDbKey[A]): DbKeyPrefix[A, A] = new DbKeyPrefixWithEncoder(encoder)
}

trait DbKeyPrefixPriority2Implicits extends DbKeyPrefixPriority3Implicits {
  // e.g
  // T as prefix of:
  // case class Bar(one: T, two: Boolean, three: Double)
  implicit def anyToDbKeyPrefixOfProduct[A, B <: Product, F <: HList, T <: HList](
    implicit
    f: DbKey.Aux[B, F],
    t: IsHCons.Aux[F, A, T],
    encoder: ToDbKey[A]
  ): DbKeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][anyToDbKeyPrefixOfProduct] ${f.describe}")
    t.unused()
    new DbKeyPrefixWithEncoder[A, B](encoder)
  }

}

trait DbKeyPrefixPriority1Implicits extends DbKeyPrefixPriority2Implicits {
  //  implicit def selfDbKeyPrefix[A <: Product](implicit encoder: ToDbKey[A]): DbKeyPrefix[A, A] = new DbKeyPrefixWithEncoder(encoder)

  // e.g
  // case class Foo(one: String, two: Boolean)
  //    as prefix of:
  // case class Bar(one: String, two: Boolean, three: Double)
  implicit def productToDbKeyPrefix[A <: Product, B <: Product, P <: HList, F <: HList, N <: Nat, T <: HList](
    implicit
    g: Generic.Aux[A, P],
    l: Length.Aux[P, N],
    f: DbKey.Aux[B, F],
    t: Take.Aux[F, N, T],
    e: P =:= T,
    encoder: ToDbKey[A]
  ): DbKeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][productToDbKeyPrefix] ${f.describe}")
    //    n.unused()
    g.unused()
    l.unused()
    t.unused()
    e.unused()
    new DbKeyPrefixWithEncoder[A, B](encoder)
  }

  // e.g
  // String :: Boolean :: HNil
  //    as prefix of:
  // case class Bar(one: String, two: Boolean, three: Double)
  implicit def hlistToDbKeyPrefix[A <: HList, B <: Product, F <: HList, N <: Nat, T <: HList](
    implicit
    l: Length.Aux[A, N],
    f: DbKey.Aux[B, F],
    t: Take.Aux[F, N, T],
    e: A =:= T,
    encoder: ToDbKey[A]
  ): DbKeyPrefix[A, B] = {
    logger.debug(s"[DbKeyPrefix][hlistToDbKeyPrefix] ${f.describe}")
    l.unused()
    t.unused()
    e.unused()
    new DbKeyPrefixWithEncoder[A, B](encoder)
  }
}

object DbKeyPrefix extends DbKeyPrefixPriority1Implicits {
  def apply[A, B](implicit e: DbKeyPrefix[A, B]): DbKeyPrefix[A, B] = e

  implicit val literalStringDbKeyPrefix: DbKeyPrefix[String, String] = (a: String) =>
    KvdbSerdesUtils.stringToByteArray(a)
}
