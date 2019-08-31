package dev.chopsticks.kvdb.codec

trait KeySerializer[T] {
  type Codec

  def encode(key: T): Array[Byte]
}

object KeySerializer {
  type Aux[T, C] = KeySerializer[T] {
    type Codec = C
  }
}
