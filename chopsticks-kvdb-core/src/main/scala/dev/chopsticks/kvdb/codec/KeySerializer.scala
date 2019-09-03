package dev.chopsticks.kvdb.codec

trait KeySerializer[T] {
  type Codec <: KeyCodec

  def serialize(key: T): Array[Byte]
}

object KeySerializer {
  type Aux[T, C <: KeyCodec] = KeySerializer[T] {
    type Codec = C
  }
}
