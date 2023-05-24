package dev.chopsticks.kvdb.fdb

import akka.actor.ActorRef
import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.{FDBException, KeyValue}
import com.apple.foundationdb.async.AsyncIterator
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair}
import dev.chopsticks.kvdb.util.KvdbCloseSignal
import dev.chopsticks.stream.GraphStageWithActorLogic
import squants.information.Information

import java.util.concurrent.TimeUnit
import scala.collection.mutable

object FdbIterateSourceStage {
  sealed trait StateMessage
  private[chopsticks] object IteratorComplete extends StateMessage
  final private[chopsticks] case class IteratorNext(keyValue: KeyValue) extends StateMessage
  final private[chopsticks] case class IteratorFailure(exception: Throwable) extends StateMessage
  private[chopsticks] object DownstreamPull extends StateMessage
  private[chopsticks] object DownstreamFinish extends StateMessage

  final private[chopsticks] class BatchEmitter(
    actor: ActorRef,
    iterator: AsyncIterator[KeyValue],
    maxBatchBytes: Int,
    keyValidator: Array[Byte] => Boolean,
    keyTransformer: Array[Byte] => Array[Byte],
    emit: KvdbBatch => Unit
  ) {
    private val reusableBuffer: mutable.ArrayBuilder[KvdbPair] = {
      val b = Array.newBuilder[KvdbPair]
      b.sizeHint(1000)
      b
    }

    private var lastKey: Array[Byte] = null.asInstanceOf[Array[Byte]]

    def maybeLastKey: Option[Array[Byte]] = Option(lastKey)

    def batchAndEmit(initialPair: Option[KvdbPair]): Int = {
      var accumulatedBatchSize = 0
      var continueBatching = true
      var isCompleted = false

      initialPair.foreach {
        case (key, value) =>
          if (keyValidator(key)) {
            accumulatedBatchSize += key.length + value.length
            lastKey = key
            val _ = reusableBuffer += keyTransformer(key) -> value
          }
          else {
            isCompleted = true
          }
      }

      while (!isCompleted && continueBatching && accumulatedBatchSize < maxBatchBytes) {
        val hasNextFuture = iterator.onHasNext()

        continueBatching =
          if (hasNextFuture.isDone && !hasNextFuture.isCompletedExceptionally) {
            if (hasNextFuture.get()) {
              val kv = iterator.next()
              val key = kv.getKey
              val value = kv.getValue

              if (keyValidator(key)) {
                accumulatedBatchSize += key.length + value.length
                lastKey = key
                val _ = reusableBuffer += keyTransformer(key) -> value
                true
              }
              else {
                isCompleted = true
                false
              }
            }
            else {
              isCompleted = true
              false
            }
          }
          else {
            if (accumulatedBatchSize == 0) {
              val _ = hasNextFuture
                .orTimeout(6, TimeUnit.SECONDS)
                .whenComplete { (hasNext, exception) =>
                  if (exception eq null) {
                    if (hasNext) actor ! IteratorNext(iterator.next())
                    else actor ! IteratorComplete
                  }
                  else {
                    actor ! IteratorFailure(exception)
                  }
                }
            }

            false
          }
      }

      val pairs = reusableBuffer.result()
      reusableBuffer.clear()
      val emitCount = pairs.length

      if (emitCount > 0) {
        emit(pairs)
      }

      if (isCompleted) {
        actor ! IteratorComplete
      }

      emitCount
    }
  }
}

final class FdbIterateSourceStage(
  initialRange: KvdbKeyRange,
  iterate: (Boolean, KvdbKeyRange) => (AsyncIterator[KeyValue], () => Unit),
  keyValidator: Array[Byte] => Boolean,
  keyTransformer: Array[Byte] => Array[Byte],
  shutdownSignal: KvdbCloseSignal,
  maxBatchBytes: Information,
  disableIsolationGuarantee: Boolean
) extends GraphStage[SourceShape[KvdbBatch]] {
  import FdbIterateSourceStage._

  private val maxBatchBytesInt = maxBatchBytes.toBytes.toInt
  private val out: Outlet[KvdbBatch] = Outlet[KvdbBatch]("FdbIterateSourceStage.out")
  override val shape: SourceShape[KvdbBatch] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = shutdownSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with GraphStageWithActorLogic {
      private var batchEmitter: BatchEmitter = _
      private var close: () => Unit = _
      private var range: KvdbKeyRange = initialRange

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            self ! DownstreamPull
          }
          override def onDownstreamFinish(cause: Throwable): Unit = {
            completeStage()
          }
        }
      )

      override def preStart(): Unit = {
        super.preStart()
        val _ = getStageActor(makeHandler({
          case DownstreamPull =>
            val _ = batchEmitter.batchAndEmit(None)

          case IteratorComplete =>
            completeStage()

          case IteratorNext(kv) =>
            val _ = batchEmitter.batchAndEmit(Some(kv.getKey -> kv.getValue))

          case IteratorFailure(ex: FDBException)
              if disableIsolationGuarantee && (ex.getCode == 1007 /* Transaction too old */ || ex.getCode == 1009 /* Request for future version */ ) =>
            close()
            val maybeLastKey = batchEmitter.maybeLastKey
            range = range.withFrom(
              maybeLastKey.fold(range.from) { lastKey =>
                KvdbKeyConstraint(Operator.GREATER, ByteString.copyFrom(lastKey)) :: range.from.tail
              }
            )

            val (newIterator, newClose) = iterate(false, range)
            close = newClose
            batchEmitter = new BatchEmitter(
              actor = self,
              iterator = newIterator,
              maxBatchBytes = maxBatchBytesInt,
              keyValidator = keyValidator,
              keyTransformer = keyTransformer,
              emit = emit(out, _)
            )
            self ! DownstreamPull

          case IteratorFailure(ex) =>
            failStage(ex)
        }))

        val (initialIterator, closeTransaction) = iterate(true, range)
        close = closeTransaction
        batchEmitter = new BatchEmitter(
          actor = self,
          iterator = initialIterator,
          maxBatchBytes = maxBatchBytesInt,
          keyValidator = keyValidator,
          keyTransformer = keyTransformer,
          emit = emit(out, _)
        )
      }

      override def postStop(): Unit = {
        try {
          if (close != null) close()
        }
        finally {
          super.postStop()
          shutdownListener.unregister()
        }
      }
    }
  }
}
