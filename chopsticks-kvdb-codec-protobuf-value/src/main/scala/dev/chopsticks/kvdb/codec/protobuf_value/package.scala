package dev.chopsticks.kvdb.codec

import scalapb._

package object protobuf_value {
  implicit def protobufMessageValueSerdes[T <: GeneratedMessage: GeneratedMessageCompanion]: ValueSerdes[T] =
    new ProtobufValueSerdes[T]
  implicit def protobufEnumValueSerdes[T <: GeneratedEnum: GeneratedEnumCompanion]: ValueSerdes[T] =
    new ProtobufEnumValueSerdes[T]
  implicit def protobufSealedOneofValueSerdes[T <: GeneratedSealedOneof, M <: T#MessageType](implicit
    typeMapper: TypeMapper[M, T],
    comp: GeneratedMessageCompanion[M]
  ): ValueSerdes[T] = new ProtobufSealedOneofValueSerdes[T, M]

  /*  implicit def dbKeyToDbValue[T](implicit dbKey: DbKey[T]): DbValue[T] = new DbValue[T] {
      import cats.syntax.either._
      def decode(bytes: Array[Byte]): DbValueDecodeResult[T] =
        dbKey.decode(bytes).leftMap(e => DbValueGenericDecodingException(e.getMessage, e))
      def encode(value: T): Array[Byte] = dbKey.encode(value)
    }*/
}
