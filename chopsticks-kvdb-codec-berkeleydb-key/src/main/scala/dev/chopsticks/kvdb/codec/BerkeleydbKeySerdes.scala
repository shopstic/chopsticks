package dev.chopsticks.kvdb.codec

import com.sleepycat.bind.tuple.{TupleInput, TupleOutput}
import dev.chopsticks.kvdb.codec.KeyDeserializer.KeyDeserializationResult
import dev.chopsticks.kvdb.util.KvdbSerdesUtils

import java.time.{LocalDateTime, ZoneId}

object BerkeleydbKeySerdes {
  def createLocalDateTimeKeySerdes(zoneId: ZoneId)
    : PredefinedBerkeleydbKeySerializer[LocalDateTime] with BerkeleydbKeyDeserializer[LocalDateTime] = {
    new PredefinedBerkeleydbKeySerializer[LocalDateTime] with BerkeleydbKeyDeserializer[LocalDateTime] {
      private val deserializer = BerkeleydbKeyDeserializer.createTry((in: TupleInput) =>
        KvdbSerdesUtils.epochNanosToLocalDateTime(BigInt(in.readBigInteger()), zoneId)
      )
      override def serialize(tupleOutput: TupleOutput, value: LocalDateTime): TupleOutput = {
        tupleOutput.writeBigInteger(KvdbSerdesUtils.localDateTimeToEpochNanos(value, zoneId).underlying)
      }
      override def deserialize(in: TupleInput): KeyDeserializationResult[LocalDateTime] = deserializer.deserialize(in)
    }
  }
}
