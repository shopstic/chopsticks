package dev.chopsticks.kvdb.util

import dev.chopsticks.kvdb.util.KvdbTailSourceGraph.EmptyTail

object KvdbAliases {
  type KvdbPair = (Array[Byte], Array[Byte])
  type KvdbBatch = Array[KvdbPair]
  type KvdbValueBatch = Array[Array[Byte]]
  type KvdbTailBatch = Either[EmptyTail, KvdbBatch]
  type KvdbTailValueBatch = Either[EmptyTail, KvdbValueBatch]
  type KvdbIndexedTailBatch = (Int, KvdbTailBatch)
}
