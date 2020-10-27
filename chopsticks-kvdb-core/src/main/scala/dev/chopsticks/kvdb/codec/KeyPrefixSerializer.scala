package dev.chopsticks.kvdb.codec

trait KeyPrefixSerializer[T] {
  def serializePrefix[P](prefix: P)(implicit ev: KeyPrefixEvidence[P, T]): Array[Byte]
}

object KeyPrefixSerializer {
  def apply[P](implicit prefixSerializer: KeyPrefixSerializer[P]): KeyPrefixSerializer[P] = prefixSerializer
}
