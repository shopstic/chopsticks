package dev.chopsticks.kvdb.codec

trait KeySerializer[T] {
  def serialize(key: T): Array[Byte]
}
