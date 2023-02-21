package dev.chopsticks.kvdb.codec

trait KeySerializer[A]:
  def serialize(key: A): Array[Byte]

object KeySerializer:
  def apply[A](using serializer: KeySerializer[A]): KeySerializer[A] = serializer
