package dev.chopsticks.kvdb.codec

import java.time.Instant

import dev.chopsticks.kvdb.codec.DbValue.create
import dev.chopsticks.kvdb.util.KvdbSerdesUtils
import scalapb._

package object protobuf_value {

  implicit def protobufMessageDbValue[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion]: DbValue[T] =
    new ProtobufDbValue[T]
  implicit def protobufEnumDbValue[T <: GeneratedEnum: GeneratedEnumCompanion]: DbValue[T] = new ProtobufEnumDbValue[T]
  implicit val instantDbValue: DbValue[Instant] = create[Instant](v => {
    KvdbSerdesUtils.instantToEpochNanos(v).toByteArray
  }, v => {
    Right(KvdbSerdesUtils.epochNanosToInstant(BigInt(v)))
  })
}
