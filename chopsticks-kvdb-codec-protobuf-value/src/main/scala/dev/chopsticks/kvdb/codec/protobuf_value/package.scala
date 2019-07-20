package dev.chopsticks.kvdb.codec

import scalapb._

package object protobuf_value {

  implicit def protobufMessageDbValue[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion]: DbValue[T] =
    new ProtobufDbValue[T]
  implicit def protobufEnumDbValue[T <: GeneratedEnum: GeneratedEnumCompanion]: DbValue[T] = new ProtobufEnumDbValue[T]

  /*  implicit def dbKeyToDbValue[T](implicit dbKey: DbKey[T]): DbValue[T] = new DbValue[T] {
      import cats.syntax.either._
      def decode(bytes: Array[Byte]): DbValueDecodeResult[T] =
        dbKey.decode(bytes).leftMap(e => DbValueGenericDecodingException(e.getMessage, e))
      def encode(value: T): Array[Byte] = dbKey.encode(value)
    }*/
}
