package dev.chopsticks.kvdb.codec

import com.apple.foundationdb.tuple.Tuple
import dev.chopsticks.kvdb.codec.KeyDeserializer.DecodingFailure
import dev.chopsticks.kvdb.util.KvdbSerdesUtils

import java.time.{LocalDateTime, ZoneId}

object FdbKeySerdes {
  def createLocalDateTimeKeySerdes(zoneId: ZoneId)
    : PredefinedFdbKeySerializer[LocalDateTime] with FdbKeyDeserializer[LocalDateTime] = {
    new PredefinedFdbKeySerializer[LocalDateTime] with FdbKeyDeserializer[LocalDateTime] {
      private val deserializer = FdbKeyDeserializer.createTry(in => {
        val epochNanos = in.getBigInteger
        KvdbSerdesUtils.epochNanosToLocalDateTime(BigInt(epochNanos), zoneId)
      })
      override def serialize(o: Tuple, t: LocalDateTime): Tuple = {
        o.add(KvdbSerdesUtils.localDateTimeToEpochNanos(t, zoneId).underlying)
      }
      override def deserialize(in: FdbTupleReader): Either[DecodingFailure, LocalDateTime] = {
        deserializer.deserialize(in)
      }
    }
  }
}
