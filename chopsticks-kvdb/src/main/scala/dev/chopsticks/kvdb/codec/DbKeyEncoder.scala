package dev.chopsticks.kvdb.codec

trait DbKeyEncoder[T] {
  type Codec

  def encode(key: T): Array[Byte]
}

object DbKeyEncoder {
  type Aux[T, C] = DbKeyEncoder[T] {
    type Codec = C
  }
}
