package dev.chopsticks.kvdb.util

import java.time.Instant

final case class EmptyTail(time: Instant, lastKey: Option[Array[Byte]])

object KvdbAliases {
  type KvdbPair = (Array[Byte], Array[Byte])
  type KvdbBatch = zio.Chunk[KvdbPair]
  type KvdbValueBatch = zio.Chunk[Array[Byte]]
  type KvdbTailBatch = Either[EmptyTail, KvdbBatch]
  // type KvdbTailValueBatch = Either[EmptyTail, KvdbValueBatch]
  type KvdbIndexedTailBatch = (Int, KvdbTailBatch)
}
