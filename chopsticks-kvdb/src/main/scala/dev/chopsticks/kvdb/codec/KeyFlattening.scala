package dev.chopsticks.kvdb.codec

import scala.deriving.Mirror

trait KeyFlattening[K]:
  type Flattened <: Tuple
  def flatten(value: K): Flattened

// temporarily simplified implementation: it doesn't support nested case classes
object KeyFlattening:
  type Aux[K, F <: Tuple] = KeyFlattening[K] { type Flattened = F }

  given KeyFlattening[K <: Product](using m: Mirror.ProductOf[K]): Aux[K, m.MirroredElemTypes] =
    new KeyFlattening[K]:
      override type Flattened = m.MirroredElemTypes
      override def flatten(value: K): m.MirroredElemTypes = Tuple.fromProductTyped(value)
end KeyFlattening
