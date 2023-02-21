package dev.chopsticks.kvdb.codec

import java.time.Instant

import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.kvdb.codec.KeySerdes.Aux

import scala.reflect.ClassTag

package object primitive {
  implicit val literalStringDbValue: ValueSerdes[String] =
    ValueSerdes.create[String](
      KvdbSerdesUtils.stringToByteArray,
      bytes => Right(KvdbSerdesUtils.byteArrayToString(bytes))
    )

  implicit val instantDbValue: ValueSerdes[Instant] =
    ValueSerdes.create[Instant](
      v => KvdbSerdesUtils.instantToEpochNanos(v).toByteArray,
      v => Right(KvdbSerdesUtils.epochNanosToInstant(BigInt(v)))
    )

  def literalStringDbKeyFor[K: ClassTag](from: String => K, to: K => String): Aux[K, EmptyTuple] =
    new KeySerdes[K]:
      type Flattened = EmptyTuple
      override def serialize(value: K): Array[Byte] =
        KvdbSerdesUtils.stringToByteArray(to(value))
      override def deserialize(bytes: Array[Byte]): KeyDeserializationResult[K] =
        Right(from(KvdbSerdesUtils.byteArrayToString(bytes)))
      override def flatten(value: K): EmptyTuple =
        ???
      override def serializeLiteralPrefix[P](prefix: P)(using ev: KeyPrefix[P, K]): Array[Byte] =
        serializePrefix(prefix)
      override def serializePrefix[P](prefix: P)(using ev: KeyPrefix[P, K]): Array[Byte] =
        prefix match
          case p: K => KvdbSerdesUtils.stringToByteArray(to(p))
          case _ => throw new IllegalStateException("Prefix type is different from key type, which is not supported")

  implicit val literalStringDbKey: Aux[String, EmptyTuple] =
    literalStringDbKeyFor[String](identity, identity)
}
