package dev.chopsticks.kvdb.codec

import java.time.Instant

import dev.chopsticks.kvdb.codec.DbKey.Aux
import dev.chopsticks.kvdb.codec.DbKeyDecoder.DbKeyDecodeResult
import dev.chopsticks.kvdb.codec.DbValue.create
import dev.chopsticks.kvdb.codec.DbValueDecoder.DbValueDecodeResult
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import shapeless._

package object primitive {
  implicit val literalStringDbValue: DbValue[String] = new DbValue[String] {
    def encode(value: String): Array[Byte] = KvdbSerdesUtils.stringToByteArray(value)
    def decode(bytes: Array[Byte]): DbValueDecodeResult[String] = Right(KvdbSerdesUtils.byteArrayToString(bytes))
  }

  implicit val instantDbValue: DbValue[Instant] = create[Instant](v => {
    KvdbSerdesUtils.instantToEpochNanos(v).toByteArray
  }, v => {
    Right(KvdbSerdesUtils.epochNanosToInstant(BigInt(v)))
  })

  def literalStringDbKeyFor[K](from: String => K, to: K => String): Aux[K, HNil] = new DbKey[K] {
    type Flattened = HNil

    def describe: String = "literalStringDbKey"

    def encode(value: K): Array[Byte] = KvdbSerdesUtils.stringToByteArray(to(value))

    def decode(bytes: Array[Byte]): DbKeyDecodeResult[K] = Right(from(KvdbSerdesUtils.byteArrayToString(bytes)))
  }

  implicit val literalStringDbKey: Aux[String, HNil] = literalStringDbKeyFor[String](identity, identity)
}
