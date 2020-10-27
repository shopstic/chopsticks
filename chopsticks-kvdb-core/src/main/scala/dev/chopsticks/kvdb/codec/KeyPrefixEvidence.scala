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
  implicit def anyToKeyPrefixOfProduct[A, B <: Product, F <: HList, T <: HList](implicit
    @nowarn f: KeySerdes.Aux[B, F],
    @nowarn t: IsHCons.Aux[F, A, T],
    @nowarn e: A <:!< Product
  ): KeyPrefixEvidence[A, B] = (prefix: A) => prefix :: HNil

  implicit def productToKeyPrefix[
    Prefix <: Product,
    Key <: Product,
    PrefixHlist <: HList,
    PrefixFlattenedHlist <: HList,
    KeyHlist <: HList,
    N <: Nat,
    TakenHlist <: HList
  ](implicit
    @nowarn neqEvidence: Prefix =:!= Key,
    prefixHlist: Generic.Aux[Prefix, PrefixHlist],
    prefixFlattenedHlist: FlatMapper.Aux[flatten.type, PrefixHlist, PrefixFlattenedHlist],
    @nowarn length: Length.Aux[PrefixFlattenedHlist, N],
    @nowarn keyHlist: KeySerdes.Aux[Key, KeyHlist],
    @nowarn takenHlist: Take.Aux[KeyHlist, N, TakenHlist],
    evidence: PrefixFlattenedHlist =:= TakenHlist
  ): KeyPrefixEvidence[Prefix, Key] = {
    (prefix: Prefix) => prefixFlattenedHlist(prefixHlist.to(prefix))
  }

  def apply[A, B](implicit e: KeyPrefixEvidence[A, B]): KeyPrefixEvidence[A, B] = e
}
