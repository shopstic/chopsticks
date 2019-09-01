package dev.chopsticks.kvdb.codec

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}

package object berkeleydb_key {
  implicit def berkeleydbKeySerializer[T](
    implicit serializer: BerkeleydbKeySerializer[T]
  ): KeySerializer.Aux[T, BerkeleyDbKeyCodec] = new KeySerializer[T] {
    type Codec = BerkeleyDbKeyCodec
    def serialize(key: T): Array[Byte] = serializer.serialize(new TupleOutput(), key).toByteArray
  }

  implicit def berkeleydbKeyDeserializer[T](implicit deserializer: BerkeleydbKeyDeserializer[T]): KeyDeserializer[T] = {
    bytes: Array[Byte] =>
      deserializer.deserialize(new TupleInput(bytes))
  }

}
