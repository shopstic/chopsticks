package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.KeySerdes.flatten
import shapeless.ops.hlist.{FlatMapper, IsHCons, Length, Take}
import shapeless.{<:!<, =:!=, Generic, HList, HNil, Nat}

import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound(msg = "Cannot prove that ${A} is a prefix of ${B}.")
trait KeyPrefixEvidence[-A, B] {
  def flatten(prefix: A): HList
}

object KeyPrefixEvidence {
  implicit def selfKeyPrefix[A](implicit serdes: KeySerdes[A]): KeyPrefixEvidence[A, A] =
    (prefix: A) => serdes.flatten(prefix)

  // e.g
  // T as prefix of:
  // case class Bar(one: T, two: Boolean, three: Double)
  implicit def anyToKeyPrefixOfProduct[Prefix, Key <: Product, KeyHList <: HList, KeyFlattened <: HList, T <: HList](
    implicit
    @nowarn e: Prefix <:!< Product,
    @nowarn keyHlist: Generic.Aux[Key, KeyHList],
    @nowarn keyFlattened: FlatMapper.Aux[flatten.type, KeyHList, KeyFlattened],
    @nowarn isHcons: IsHCons.Aux[KeyFlattened, Prefix, T]
  ): KeyPrefixEvidence[Prefix, Key] = (prefix: Prefix) => prefix :: HNil

  implicit def productToKeyPrefix[
    Prefix <: Product,
    Key <: Product,
    PrefixHList <: HList,
    PrefixFlattened <: HList,
    KeyHList <: HList,
    KeyFlattened <: HList,
    N <: Nat,
    TakenHList <: HList
  ](implicit
    @nowarn neqEvidence: Prefix =:!= Key,
    prefixHList: Generic.Aux[Prefix, PrefixHList],
    prefixFlattened: FlatMapper.Aux[flatten.type, PrefixHList, PrefixFlattened],
    @nowarn keyHList: Generic.Aux[Key, KeyHList],
    @nowarn keyFlattened: FlatMapper.Aux[flatten.type, KeyHList, KeyFlattened],
    @nowarn length: Length.Aux[PrefixFlattened, N],
    @nowarn takenHList: Take.Aux[KeyFlattened, N, TakenHList],
    evidence: PrefixFlattened =:= TakenHList
  ): KeyPrefixEvidence[Prefix, Key] = {
    (prefix: Prefix) => prefixFlattened(prefixHList.to(prefix))
  }

  def apply[A, B](implicit e: KeyPrefixEvidence[A, B]): KeyPrefixEvidence[A, B] = e
}
