package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult
import dev.chopsticks.kvdb.codec.ValueDeserializer.ValueDeserializationResult
import dev.chopsticks.kvdb.codec.{KeySerdes, ValueSerdes}
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import dev.chopsticks.kvdb.util.KvdbUtils

abstract class ColumnFamily[K: KeySerdes, V: ValueSerdes] {
  lazy val id: String = KvdbUtils.deriveColumnFamilyId(this.getClass)

  def keySerdes: KeySerdes[K] = implicitly[KeySerdes[K]]
  def valueSerdes: ValueSerdes[V] = implicitly[ValueSerdes[V]]

  def serializeKey(key: K): Array[Byte] = keySerdes.serialize(key)
  def deserializeKey(bytes: Array[Byte]): KeyDeserializationResult[K] = keySerdes.deserialize(bytes)

  def serializeValue(value: V): Array[Byte] = valueSerdes.serialize(value)
  def deserializeValue(bytes: Array[Byte]): ValueDeserializationResult[V] = valueSerdes.deserialize(bytes)

  def unsafeDeserializeKey(bytes: Array[Byte]): K = {
    deserializeKey(bytes) match {
      case Right(v) => v
      case Left(e) => throw e
    }
  }

  def unsafeDeserializeValue(bytes: Array[Byte]): V = {
    deserializeValue(bytes) match {
      case Right(v) => v
      case Left(e) => throw e
    }
  }

  def unsafeDeserialize(pair: KvdbPair): (K, V) = {
    (unsafeDeserializeKey(pair._1), unsafeDeserializeValue(pair._2))
  }
}
