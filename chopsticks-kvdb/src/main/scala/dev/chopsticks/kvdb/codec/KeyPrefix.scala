package dev.chopsticks.kvdb.codec

import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound(msg = "Cannot prove that ${A} is a prefix of ${B}")
trait KeyPrefix[A, B]:
  type Repr <: Tuple
  def flatten(prefix: A): Repr

object KeyPrefix extends LowPriorityKeyPrefixInstances:

  given identityPrefix[A, Flattened <: Tuple](using
    flattening: KeyFlattening.Aux[A, Flattened]
  ): KeyPrefix[A, A] =
    new KeyPrefix[A, A]:
      type Repr = flattening.Flattened
      override def flatten(prefix: A): Repr = flattening.flatten(prefix)

trait LowPriorityKeyPrefixInstances:

  type TypePrefix[P <: Tuple, K <: Tuple] <: Tuple =
    P match
      case EmptyTuple => P
      case ph *: pt =>
        K match
          case EmptyTuple => Nothing
          case `ph` *: kt =>
            ph *: TypePrefix[pt, kt]

  given KeyPrefix[Prefix <: Tuple, Key <: Tuple](using _ <:< TypePrefix[Prefix, Key]): KeyPrefix[Prefix, Key] =
    new KeyPrefix[Prefix, Key]:
      type Repr = Prefix
      override def flatten(prefix: Prefix): Repr = prefix

  inline given [Prefix <: Product, Key <: Product](using
    prefixFlattening: KeyFlattening[Prefix],
    keyFlattening: KeyFlattening[Key]
  ): KeyPrefix[Prefix, Key] =
    val _ = summon[KeyPrefix[prefixFlattening.Flattened, keyFlattening.Flattened]]
    new KeyPrefix[Prefix, Key]:
      type Repr = prefixFlattening.Flattened
      override def flatten(prefix: Prefix): Repr = prefixFlattening.flatten(prefix)

  // e.g
  // T as prefix of:
  // case class Bar(one: T, two: Boolean, three: Double)
  given [Prefix, Key <: Product, KeyTailFlattened <: Tuple](
    using @nowarn ev: KeyFlattening.Aux[Key, Prefix *: KeyTailFlattened]
  ): KeyPrefix[Prefix, Key] =
    new KeyPrefix[Prefix, Key]:
      type Repr = Prefix *: EmptyTuple
      override def flatten(prefix: Prefix): Repr = prefix *: EmptyTuple
