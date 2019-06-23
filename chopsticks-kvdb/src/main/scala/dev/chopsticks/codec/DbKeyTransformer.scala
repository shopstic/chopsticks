package dev.chopsticks.codec

import io.scalaland.chimney.{Transformer => ChimneyTransformer}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find implicit DbKeyTransformer from ${From} to ${To}.")
trait DbKeyTransformer[From, To] {
  def transform(from: From): To
}

trait DerivedDbKeyTransformer[From, To] extends DbKeyTransformer[From, To]

object DbKeyTransformer {
  def apply[From, To](implicit t: DerivedDbKeyTransformer[From, To]): DbKeyTransformer[From, To] = t

  def transform[From, To](from: From)(implicit t: DbKeyTransformer[From, To]): To = t.transform(from)

  implicit def identityTransformer[V]: DerivedDbKeyTransformer[V, V] = v => v
  implicit def autoTransformer[From, To](
    implicit d: ChimneyTransformer[From, To]
  ): DerivedDbKeyTransformer[From, To] = { v =>
    d.transform(v)
  }

  type ToStringDbKeyTransformer[From] = DbKeyTransformer[From, String]

  object Dsl {
    implicit class DbKeyTransformerOps[V](value: V) {
      def transformToDbKey[T](implicit t: DbKeyTransformer[V, T]): T = t.transform(value)
    }
  }
}
