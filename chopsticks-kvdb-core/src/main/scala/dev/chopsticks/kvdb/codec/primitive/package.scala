package dev.chopsticks.kvdb.codec

import java.time.Instant

import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import dev.chopsticks.kvdb.codec.KeySerdes.Aux
import shapeless._

package object primitive {
  implicit val literalStringDbValue: ValueSerdes[String] = ValueSerdes.create[String](KvdbSerdesUtils.stringToByteArray, bytes => Right(KvdbSerdesUtils.byteArrayToString(bytes)))

  implicit val instantDbValue: ValueSerdes[Instant] = ValueSerdes.create[Instant](v => {
    KvdbSerdesUtils.instantToEpochNanos(v).toByteArray
  }, v => {
    Right(KvdbSerdesUtils.epochNanosToInstant(BigInt(v)))
  })

  def literalStringDbKeyFor[K](from: String => K, to: K => String): Aux[K, HNil, PrimitiveDbKeyCodec] = new KeySerdes[K] {
    type Flattened = HNil
    type Codec = PrimitiveDbKeyCodec

    def describe: String = "literalStringDbKey"

    def serialize(value: K): Array[Byte] = KvdbSerdesUtils.stringToByteArray(to(value))

    def decode(bytes: Array[Byte]): KeyDeserializationResult[K] = Right(from(KvdbSerdesUtils.byteArrayToString(bytes)))
  }

  implicit val literalStringDbKey: Aux[String, HNil, PrimitiveDbKeyCodec] =
    literalStringDbKeyFor[String](identity, identity)
}
