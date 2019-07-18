package dev.chopsticks.kvdb.codec

trait DbValueEncoder[T] {
  def encode(value: T): Array[Byte]
}
