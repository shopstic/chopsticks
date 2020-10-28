package dev.chopsticks.kvdb.codec

trait KeySerializer[-T] {
  def serialize(key: T): Array[Byte]
}

object KeySerializer {
  def apply[P](implicit serializer: KeySerializer[P]): KeySerializer[P] = serializer
}
