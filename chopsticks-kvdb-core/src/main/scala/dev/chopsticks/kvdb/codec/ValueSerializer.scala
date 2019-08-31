package dev.chopsticks.kvdb.codec

trait ValueSerializer[T] {
  def serialize(value: T): Array[Byte]
}
