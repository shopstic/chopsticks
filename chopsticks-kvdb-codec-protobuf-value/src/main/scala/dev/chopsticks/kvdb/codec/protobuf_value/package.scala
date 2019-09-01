package dev.chopsticks.kvdb.codec

import scalapb._

package object protobuf_value {

  implicit def protobufMessageValueSerdes[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion]: ValueSerdes[T] =
    new ProtobufValueSerdes[T]
  implicit def protobufEnumValueSerdes[T <: GeneratedEnum: GeneratedEnumCompanion]: ValueSerdes[T] = new ProtobufEnumValueSerdes[T]

  /*  implicit def dbKeyToDbValue[T](implicit dbKey: DbKey[T]): DbValue[T] = new DbValue[T] {
      import cats.syntax.either._
      def decode(bytes: Array[Byte]): DbValueDecodeResult[T] =
        dbKey.decode(bytes).leftMap(e => DbValueGenericDecodingException(e.getMessage, e))
      def encode(value: T): Array[Byte] = dbKey.encode(value)
    }*/
}
