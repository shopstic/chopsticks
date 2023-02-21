package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.async.AsyncIterator
import com.apple.foundationdb.{FDBException, KeyValue}
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.fdb.FdbBatchEmitter.FdbBatchEmitterSignal
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbTailBatch
import dev.chopsticks.kvdb.util.{EmptyTail, KvdbCloseSignal}
import squants.information.Information
import zio.stream.*
import zio.{Ref, ZIO}

import java.time.{Duration, Instant}

object FdbTailStream:

  final private[fdb] case class FdbTailStreamState(
    lastKey: Array[Byte],
    iterator: AsyncIterator[KeyValue],
    close: () => Unit,
    range: KvdbKeyRange,
    pollingDelay: Duration,
    applyDelayAndRecreateTx: Boolean
  ):
    def maybeLastKey: Option[Array[Byte]] = Option(lastKey)
  end FdbTailStreamState

  def tailStream(
    initialRange: KvdbKeyRange,
    iterate: KvdbKeyRange => (AsyncIterator[KeyValue], () => Unit),
    keyValidator: Array[Byte] => Boolean,
    keyTransformer: Array[Byte] => Array[Byte],
    shutdownSignal: KvdbCloseSignal, // todo we need to use it
    maxBatchBytes: Information,
    tailPollingInterval: Duration,
//    tailPollingBackoffFactor: Double Refined Greater[W.`1.0d`.T] = 1.15d
    tailPollingBackoffFactor: Double = 1.15d
  ): Stream[Throwable, KvdbTailBatch] =
    import FdbBatchEmitterSignal._

    val maxBatchBytesInt = maxBatchBytes.toBytes.toInt
    def makeEffect(
      stateRef: Ref[FdbTailStreamState],
      emitEmptyTailAndKeepTailing: FdbTailStreamState => ZIO[Any, Nothing, Left[EmptyTail, Nothing]]
    ): ZIO[Any, Option[Throwable], KvdbTailBatch] = {
      stateRef.get.flatMap { currentState =>
        for
          state <-
            if !currentState.applyDelayAndRecreateTx then ZIO.succeed(currentState)
            else
              ZIO
                .succeed {
                  currentState.close()
                }
                .flatMap { _ =>
                  ZIO
                    .succeed {
                      iterate(currentState.range)
                    }
                    .flatMap { case (iterator, close) =>
                      val newState =
                        currentState.copy(applyDelayAndRecreateTx = false, iterator = iterator, close = close)
                      //                  stateRef.setAsync(newState).as(newState)
                      stateRef.set(newState).as(newState)
                    }
                    .delay(currentState.pollingDelay)
                }
          signal <- FdbBatchEmitter.batchAndEmit(
            maxBatchBytesSize = maxBatchBytesInt,
            lastSeenKey = state.lastKey,
            iterator = state.iterator,
            keyValidator = keyValidator,
            keyTransformer = keyTransformer
          )
          result <- {
            val io: ZIO[Any, Option[Throwable], KvdbTailBatch] = signal match
              case FdbBatchEmitterSignal.IteratorComplete =>
                state.close()
                emitEmptyTailAndKeepTailing(state)
              case FdbBatchEmitterSignal.NextBatch(values, lastSeenKey) =>
                val setStateIo = stateRef.set(state.copy(lastKey = lastSeenKey))
                if (values.nonEmpty) setStateIo.as(Right(values))
                else setStateIo *> makeEffect(stateRef, emitEmptyTailAndKeepTailing)
              case FdbBatchEmitterSignal.Failure(ex: FDBException, lastSeenKey)
                  if ex.getCode == 1007 /* Transaction too old */ ||
                    ex.getCode == 1009 /* Request for future version */ ||
                    ex.getCode == 1037 /* process_behind */ =>
                val newState = state.copy(lastKey = lastSeenKey)
                // todo maybe we should resume right away instead
                emitEmptyTailAndKeepTailing(newState)
              case FdbBatchEmitterSignal.Failure(ex, _) =>
                state.close()
                ZIO.fail(Some(ex))
            io
          }
        yield result
      }
    }
    for
      stateRef <- ZStream.fromZIO {
        zio.Ref.make {
          val (initialIterator, closeTransaction) = iterate(initialRange)
          FdbTailStreamState(
            lastKey = null,
            iterator = initialIterator,
            close = closeTransaction,
            range = initialRange,
            pollingDelay = Duration.ZERO,
            applyDelayAndRecreateTx = false
          )
        }
      }
      emitEmptyTailAndKeepTailing = (state: FdbTailStreamState) => {
        val newPollingDelay =
          if state.pollingDelay == Duration.ZERO then Duration.ofMillis(1)
          else if state.pollingDelay.compareTo(tailPollingInterval) < 0 then
            Duration.ofNanos((state.pollingDelay.toNanos * tailPollingBackoffFactor).toLong)
          else tailPollingInterval
        val emptyTail = EmptyTail(Instant.now, state.maybeLastKey)
        val maybeLastKey = Option(state.lastKey)
        val newRange = state.range.withFrom(
          maybeLastKey.fold(state.range.from) { lastKey =>
            KvdbKeyConstraint(Operator.GREATER, ByteString.copyFrom(lastKey)) :: state.range.from.tail
          }
        )
        val newState = state.copy(pollingDelay = newPollingDelay, applyDelayAndRecreateTx = true, range = newRange)
        stateRef.set(newState).as(Left(emptyTail))
      }
      elem <- ZStream
        .repeatZIOOption(makeEffect(stateRef, emitEmptyTailAndKeepTailing))
        .ensuring {
          stateRef.get.map { state =>
            state.close()
          }
        }
    yield elem

end FdbTailStream
