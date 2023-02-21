package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.async.AsyncIterator
import com.apple.foundationdb.{FDBException, KeyValue}
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.util.KvdbCloseSignal
import dev.chopsticks.kvdb.fdb.FdbBatchEmitter.FdbBatchEmitterSignal
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import squants.information.Information
import zio.stream.*
import zio.{Chunk, Ref, ZIO}

object FdbIterateStream:

  final private[fdb] case class FdbIterateStreamState(
    lastKey: Array[Byte],
    iterator: AsyncIterator[KeyValue],
    close: () => Unit,
    range: KvdbKeyRange
  ):
    def maybeLastKey: Option[Array[Byte]] = Option(lastKey)
  end FdbIterateStreamState

  def iterateStream(
    initialRange: KvdbKeyRange,
    iterate: (Boolean, KvdbKeyRange) => (AsyncIterator[KeyValue], () => Unit),
    keyValidator: Array[Byte] => Boolean,
    keyTransformer: Array[Byte] => Array[Byte],
    shutdownSignal: KvdbCloseSignal, // todo use this (maybe it should expose a scope instead?)
    maxBatchBytes: Information,
    disableIsolationGuarantee: Boolean
  ): Stream[Throwable, Chunk[KvdbPair]] =
    val maxBatchBytesSize = maxBatchBytes.toBytes.toInt
    def makeEffect(stateRef: Ref[FdbIterateStreamState]): ZIO[Any, Option[Throwable], Chunk[KvdbPair]] = {
      for
        state <- stateRef.get
        signal <- FdbBatchEmitter
          .batchAndEmit(
            maxBatchBytesSize = maxBatchBytesSize,
            lastSeenKey = state.lastKey,
            iterator = state.iterator,
            keyValidator = keyValidator,
            keyTransformer = keyTransformer
          )
        result <- signal match
          case FdbBatchEmitterSignal.IteratorComplete =>
            state.close()
            ZIO.fail(None)
          case FdbBatchEmitterSignal.NextBatch(values, lastSeenKey) =>
            val setStateIo = stateRef.set(state.copy(lastKey = lastSeenKey))
            if (values.nonEmpty) setStateIo.as(values)
            else setStateIo *> makeEffect(stateRef)
          case FdbBatchEmitterSignal.Failure(ex: FDBException, lastSeenKeyNullable)
              if disableIsolationGuarantee && (ex.getCode == 1007 /* Transaction too old */ || ex.getCode == 1009 /* Request for future version */ ) =>
            state.close()
            val maybeLastKey = Option(lastSeenKeyNullable)
            val newRange = state.range.withFrom(
              maybeLastKey.fold(state.range.from) { lastKey =>
                KvdbKeyConstraint(Operator.GREATER, ByteString.copyFrom(lastKey)) :: state.range.from.tail
              }
            )
            val (newIterator, newClose) = iterate(false, newRange)
            stateRef.set(state.copy(
              iterator = newIterator,
              close = newClose,
              lastKey = lastSeenKeyNullable,
              range = newRange
            )) *> makeEffect(stateRef)
//              .as(Chunk.empty)
          case FdbBatchEmitterSignal.Failure(ex, _) =>
            state.close()
            ZIO.fail(Some(ex))
      yield result
    }
    for
//      shutdownListener <- ZIO.acquireRelease(ZIO.succeed(shutdownSignal.createListener())) { listener =>
//        ZIO.succeed(listener.unregister())
//      }
      stateRef <- ZStream.fromZIO {
        zio.Ref.make {
          val (initialIterator, closeTransaction) = iterate(true, initialRange)
          FdbIterateStreamState(null, initialIterator, closeTransaction, initialRange)
        }
      }
      elem <- ZStream
        .repeatZIOOption { makeEffect(stateRef) }
        .ensuring {
          stateRef.get.map { state =>
            state.close()
          }
        }
    yield elem
  end iterateStream

end FdbIterateStream
