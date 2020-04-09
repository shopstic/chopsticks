package dev.chopsticks.kvdb.fdb

import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.async.AsyncIterator
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair}
import dev.chopsticks.kvdb.util.{KvdbClientOptions, KvdbCloseSignal}
import dev.chopsticks.stream.GraphStageWithActorLogic

import scala.collection.mutable

object FdbAsyncIteratorToSourceStage {
  sealed trait IteratorState
  object IteratorComplete extends IteratorState
  final case class IteratorNext(keyValue: KeyValue) extends IteratorState
  final case class IteratorFailure(exception: Throwable) extends IteratorState
}

final class FdbAsyncIteratorToSourceStage(
  iterator: AsyncIterator[KeyValue],
  keyValidator: Array[Byte] => Boolean,
  keyTransformer: Array[Byte] => Array[Byte],
  onClose: () => Unit,
  shutdownSignal: KvdbCloseSignal
)(implicit clientOptions: KvdbClientOptions)
    extends GraphStage[SourceShape[KvdbBatch]] {
  import FdbAsyncIteratorToSourceStage._

  private val maxBatchBytes = clientOptions.maxBatchBytes.toBytes.toInt
  private val out: Outlet[KvdbBatch] = Outlet[KvdbBatch]("FdbAsyncIteratorToSourceStage.out")
  override val shape: SourceShape[KvdbBatch] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = shutdownSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with GraphStageWithActorLogic {
      private val reusableBuffer: mutable.ArrayBuilder[KvdbPair] = {
        val b = Array.newBuilder[KvdbPair]
        b.sizeHint(1000)
        b
      }

      private def batchAndEmit(initialPair: Option[KvdbPair]): Unit = {
        var accumulatedBatchSize = 0
        var continueBatching = true
        var isCompleted = false

        initialPair.foreach {
          case (key, value) =>
            if (keyValidator(key)) {
              accumulatedBatchSize += key.length + value.length
              val _ = reusableBuffer += keyTransformer(key) -> value
            }
            else {
              isCompleted = true
            }
        }

        while (!isCompleted && continueBatching && accumulatedBatchSize < maxBatchBytes) {
          val hasNextFuture = iterator.onHasNext()

          continueBatching = if (hasNextFuture.isDone && !hasNextFuture.isCompletedExceptionally) {
            if (hasNextFuture.get()) {
              val kv = iterator.next()
              val key = kv.getKey
              val value = kv.getValue

              if (keyValidator(key)) {
                accumulatedBatchSize += key.length + value.length
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
              val _ = hasNextFuture.whenComplete { (hasNext, exception) =>
                if (exception eq null) {
                  if (hasNext) self ! IteratorNext(iterator.next())
                  else self ! IteratorComplete
                }
                else {
                  self ! IteratorFailure(exception)
                }
              }
            }

            false
          }
        }

        if (accumulatedBatchSize > 0) {
          emit(out, reusableBuffer.result())
          reusableBuffer.clear()
        }

        if (isCompleted) {
          self ! IteratorComplete
        }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            batchAndEmit(None)
          }
        }
      )

      override def preStart(): Unit = {
        val _ = getStageActor(makeHandler({
          case IteratorComplete =>
            completeStage()
          case IteratorNext(kv) =>
            batchAndEmit(Some(kv.getKey -> kv.getValue))
          case IteratorFailure(ex) =>
            failStage(ex)
        }))
      }

      override def postStop(): Unit = {
        try {
          onClose()
        }
        finally {
          shutdownListener.unregister()
          super.postStop()
        }
      }
    }
  }
}
