package dev.chopsticks.kvdb.codec

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find implicit KeyTransformer from ${From} to ${To}.")
trait KeyTransformer[From, To] {
  def transform(from: From): To
}

trait DerivedKeyTransformer[From, To] extends KeyTransformer[From, To]

// todo add proper implementation based on Scala 3 meta-programming features
object KeyTransformer {
  def apply[From, To](implicit t: DerivedKeyTransformer[From, To]): KeyTransformer[From, To] = t

  def transform[From, To](from: From)(using t: KeyTransformer[From, To]): To = t.transform(from)

  given identityTransformer[V]: DerivedKeyTransformer[V, V] = v => v
  // implicit def autoTransformer[From, To](implicit
  //   d: ChimneyTransformer[From, To]
  // ): DerivedKeyTransformer[From, To] = { v => d.transform(v) }

  type ToStringDbKeyTransformer[From] = KeyTransformer[From, String]

  object Dsl {
    extension [V](value: V) {
      def transformToKey[T](using t: KeyTransformer[V, T]): T = t.transform(value)
    }
  }
}
