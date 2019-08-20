package dev.chopsticks.kvdb.codec

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}

package object berkeleydb_key {
//  def literalStringDbKeyFor[K](from: String => K, to: K => String): Aux[K, HNil] = new DbKey[K] {
//    type Flattened = HNil
//
//    def describe: String = "literalStringDbKey"
//
//    def encode(value: K): Array[Byte] = KvdbSerdesUtils.stringToByteArray(to(value))
//
//    def decode(bytes: Array[Byte]): DbKeyDecodeResult[K] = Right(from(KvdbSerdesUtils.byteArrayToString(bytes)))
//  }

//  implicit val instantDbKey: Aux[Instant, Instant :: HNil] = deriveGeneric[Instant]
//  implicit val literalStringDbKey: Aux[String, HNil] = literalStringDbKeyFor[String](identity, identity)
//  implicit val dateTimeDbKey: Aux[LocalDateTime, LocalDateTime :: HNil] = deriveGeneric[LocalDateTime]
  implicit def berkeleydbKeyEncoder[T](
    implicit encoder: BerkeleydbKeyEncoder[T]
  ): DbKeyEncoder.Aux[T, BerkeleyDbKeyCodec] = new DbKeyEncoder[T] {
    type Codec = BerkeleyDbKeyCodec
    def encode(key: T): Array[Byte] = encoder.encode(new TupleOutput(), key).toByteArray
  }

  implicit def berkeleydbKeyDecoder[T](implicit decoder: BerkeleydbKeyDecoder[T]): DbKeyDecoder[T] = {
    bytes: Array[Byte] =>
      decoder.decode(new TupleInput(bytes))
  }

}
