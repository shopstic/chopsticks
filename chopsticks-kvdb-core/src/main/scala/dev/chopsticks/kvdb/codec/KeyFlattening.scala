package dev.chopsticks.kvdb.codec

import dev.chopsticks.kvdb.codec.KeySerdes.flatten
import shapeless.ops.hlist.FlatMapper
import shapeless.{<:!<, Generic, HList, HNil, :: => :::}

import scala.annotation.nowarn

trait KeyFlattening[K] {
  type Flattened <: HList
  def flatten(value: K): Flattened
}

object KeyFlattening {
  type Aux[K, F <: HList] = KeyFlattening[K] {
    type Flattened = F
  }

  implicit def nonProductFlattening[V](
    implicit @nowarn e: V <:!< Product
  ): Aux[V, V ::: HNil] = new KeyFlattening[V] {
    override type Flattened = V ::: HNil
    override def flatten(value: V): Flattened = value :: HNil
  }

  implicit def productFlattening[V <: Product, H <: HList, F <: HList](
    implicit
    generic: Generic.Aux[V, H],
    flatMapper: FlatMapper.Aux[flatten.type, H, F]
  ): Aux[V, F] = new KeyFlattening[V] {
    override type Flattened = F
    override def flatten(value: V): Flattened = flatMapper(generic.to(value))
  }
}
