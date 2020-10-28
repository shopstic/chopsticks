package dev.chopsticks.kvdb.codec

import scala.annotation.nowarn
import shapeless._
import shapeless.ops.hlist.FlatMapper

trait KeyFlattening[K] {
  type Flattened <: HList
  def flatten(value: K): Flattened
}

object KeyFlattening {
  type Aux[K, F <: HList] = KeyFlattening[K] {
    type Flattened = F
  }

  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T]: Case.Aux[T, T :: HNil] = at[T](v => v :: HNil)
  }

  object flattening extends LowPriorityFlatten {
    implicit def hlistCase[L <: HList, O <: HList](implicit
      lfm: Lazy[FlatMapper.Aux[flattening.type, L, O]]
    ): Case.Aux[L, O] = at[L](lfm.value(_))

    implicit def instanceCase[C <: Product, H <: HList, O <: HList](implicit
      gen: Lazy[Generic.Aux[C, H]],
      lfm: Lazy[FlatMapper.Aux[flattening.type, H, O]]
    ): Case.Aux[C, O] = {
      at[C](c => lfm.value(gen.value.to(c)))
    }
  }

  implicit def nonProductFlattening[V](
    implicit @nowarn e: V <:!< Product
  ): Aux[V, V :: HNil] = new KeyFlattening[V] {
    override type Flattened = V :: HNil
    override def flatten(value: V): Flattened = value :: HNil
  }

  implicit def productFlattening[V <: Product, H <: HList, F <: HList](
    implicit
    generic: Generic.Aux[V, H],
    flatMapper: FlatMapper.Aux[flattening.type, H, F]
  ): Aux[V, F] = new KeyFlattening[V] {
    override type Flattened = F
    override def flatten(value: V): Flattened = flatMapper(generic.to(value))
  }
}
