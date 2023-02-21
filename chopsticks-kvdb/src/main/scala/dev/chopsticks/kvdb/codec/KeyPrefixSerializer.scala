package dev.chopsticks.kvdb.codec

trait KeyPrefixSerializer[Key]:
  def serializePrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, Key]): Array[Byte]
  def serializeLiteralPrefix[Prefix](prefix: Prefix)(using ev: KeyPrefix[Prefix, Key]): Array[Byte]

object KeyPrefixSerializer:
  def apply[Key](using prefixSerializer: KeyPrefixSerializer[Key]): KeyPrefixSerializer[Key] = prefixSerializer
