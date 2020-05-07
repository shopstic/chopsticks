package dev.chopsticks.kvdb.codec

import io.scalaland.chimney.{Transformer => ChimneyTransformer}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find implicit DbKeyTransformer from ${From} to ${To}.")
trait KeyTransformer[From, To] {
  def transform(from: From): To
}

trait DerivedKeyTransformer[From, To] extends KeyTransformer[From, To]

object KeyTransformer {
  def apply[From, To](implicit t: DerivedKeyTransformer[From, To]): KeyTransformer[From, To] = t

  def transform[From, To](from: From)(implicit t: KeyTransformer[From, To]): To = t.transform(from)

  implicit def identityTransformer[V]: DerivedKeyTransformer[V, V] = v => v
  implicit def autoTransformer[From, To](implicit
    d: ChimneyTransformer[From, To]
  ): DerivedKeyTransformer[From, To] = { v => d.transform(v) }

  type ToStringDbKeyTransformer[From] = KeyTransformer[From, String]

  object Dsl {
    implicit class KeyTransformerOps[V](value: V) {
      def transformToKey[T](implicit t: KeyTransformer[V, T]): T = t.transform(value)
    }
  }
}
