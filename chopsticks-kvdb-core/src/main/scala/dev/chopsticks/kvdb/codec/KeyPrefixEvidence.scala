package dev.chopsticks.kvdb.codec

import shapeless.ops.hlist.{IsHCons, Length, Take}
import shapeless.{<:!<, =:!=, HList, HNil, Nat}

import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound(msg = "Cannot prove that ${A} is a prefix of ${B}.")
trait KeyPrefixEvidence[-A, B] {
  def flatten(prefix: A): HList
}

object KeyPrefixEvidence {
  implicit def selfKeyPrefix[A](implicit flattening: KeyFlattening[A]): KeyPrefixEvidence[A, A] =
    (prefix: A) => flattening.flatten(prefix)

  // e.g
  // T as prefix of:
  // case class Bar(one: T, two: Boolean, three: Double)
  implicit def nonProductToKeyPrefixOfProduct[Prefix, Key <: Product, KeyFlattened <: HList](
    implicit
    @nowarn e: Prefix <:!< Product,
    @nowarn keyFlattening: KeyFlattening.Aux[Key, KeyFlattened],
    @nowarn isHcons: IsHCons.Aux[KeyFlattened, Prefix, _]
  ): KeyPrefixEvidence[Prefix, Key] = (prefix: Prefix) => prefix :: HNil

  implicit def productToKeyPrefixOfProduct[
    Prefix <: Product,
    Key <: Product,
    PrefixFlattened <: HList,
    KeyFlattened <: HList,
    N <: Nat,
    TakenHList <: HList
  ](implicit
    @nowarn neqEvidence: Prefix =:!= Key,
    prefixFlattening: KeyFlattening.Aux[Prefix, PrefixFlattened],
    @nowarn keyFlattening: KeyFlattening.Aux[Key, KeyFlattened],
    @nowarn length: Length.Aux[PrefixFlattened, N],
    @nowarn takenHList: Take.Aux[KeyFlattened, N, TakenHList],
    evidence: PrefixFlattened =:= TakenHList
  ): KeyPrefixEvidence[Prefix, Key] = {
    (prefix: Prefix) => prefixFlattening.flatten(prefix)
  }

  def apply[A, B](implicit e: KeyPrefixEvidence[A, B]): KeyPrefixEvidence[A, B] = e
}
