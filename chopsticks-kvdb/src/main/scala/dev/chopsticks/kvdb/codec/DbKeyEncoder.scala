package dev.chopsticks.kvdb.codec

trait DbKeyEncoder[T] {
  def encode(key: T): Array[Byte]
}
