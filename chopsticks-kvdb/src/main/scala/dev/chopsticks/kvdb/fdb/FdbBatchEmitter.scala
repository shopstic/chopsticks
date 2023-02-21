package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.async.AsyncIterator
import com.apple.foundationdb.KeyValue
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import zio.{Chunk, ChunkBuilder, UIO, ZIO}

import java.util.concurrent.{CompletableFuture, TimeUnit}

object FdbBatchEmitter:

  enum FdbBatchEmitterSignal:
    case IteratorComplete
    case NextBatch(values: Chunk[KvdbPair], lastSeenKeyNullable: Array[Byte])
    case Failure(exception: Throwable, lastSeenKeyNullable: Array[Byte])

  def batchAndEmit(
    maxBatchBytesSize: Int,
    lastSeenKey: Array[Byte],
    iterator: AsyncIterator[KeyValue],
    keyValidator: Array[Byte] => Boolean,
    keyTransformer: Array[Byte] => Array[Byte]
  ): UIO[FdbBatchEmitterSignal] =
    var accumulatedBatchSize = 0
    var continueBatching = true
    var isCompleted = false

    var lastKey = lastSeenKey
    val builder = ChunkBuilder.make[KvdbPair]()
    var futureHasNext: CompletableFuture[java.lang.Boolean] = null

    while (!isCompleted && continueBatching && accumulatedBatchSize < maxBatchBytesSize) {
      val hasNextFuture = iterator.onHasNext()

      continueBatching =
        if hasNextFuture.isDone && !hasNextFuture.isCompletedExceptionally then
          if hasNextFuture.get() then
            val kv = iterator.next()
            val key = kv.getKey
            val value = kv.getValue

            if keyValidator(key) then
              accumulatedBatchSize += key.length + value.length
              lastKey = key
              val _ = builder += keyTransformer(key) -> value
              true
            else
              isCompleted = true
              false
          else
            isCompleted = true
            false
        else if accumulatedBatchSize != 0 then
          false
        else
          futureHasNext = hasNextFuture.orTimeout(6, TimeUnit.SECONDS)
          false
    }

    if futureHasNext ne null then
      ZIO
        .fromCompletableFuture(futureHasNext)
        .foldZIO(
          ex =>
            ZIO.succeed(FdbBatchEmitterSignal.Failure(ex, lastKey)),
          hasNext =>
            if hasNext then ZIO.succeed(FdbBatchEmitterSignal.NextBatch(Chunk.empty, lastKey))
            else ZIO.succeed(FdbBatchEmitterSignal.IteratorComplete)
        )
    else if accumulatedBatchSize == 0 then
      ZIO.succeed(FdbBatchEmitterSignal.IteratorComplete)
    else
      ZIO.succeed(FdbBatchEmitterSignal.NextBatch(builder.result(), lastKey))

end FdbBatchEmitter
