package dev.chopsticks.kvdb.fdb

import akka.actor.ActorRef
import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.async.AsyncIterator
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair}
import dev.chopsticks.kvdb.util.{KvdbClientOptions, KvdbCloseSignal}
import dev.chopsticks.stream.GraphStageWithActorLogic

import scala.collection.mutable

object FdbIterateSourceStage {
  sealed trait StateMessage
  private[fdb] object IteratorComplete extends StateMessage
  final private[fdb] case class IteratorNext(keyValue: KeyValue) extends StateMessage
  final private[fdb] case class IteratorFailure(exception: Throwable) extends StateMessage
  private[fdb] object DownstreamPull extends StateMessage
  private[fdb] object DownstreamFinish extends StateMessage

  final private[fdb] class BatchEmitter(
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

        continueBatching = if (hasNextFuture.isDone && !hasNextFuture.isCompletedExceptionally) {
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
            val _ = hasNextFuture.whenComplete { (hasNext, exception) =>
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
  iterator: AsyncIterator[KeyValue],
  keyValidator: Array[Byte] => Boolean,
  keyTransformer: Array[Byte] => Array[Byte],
  close: () => Unit,
  shutdownSignal: KvdbCloseSignal
)(implicit clientOptions: KvdbClientOptions)
    extends GraphStage[SourceShape[KvdbBatch]] {
  import FdbIterateSourceStage._

  private val maxBatchBytes = clientOptions.maxBatchBytes.toBytes.toInt
  private val out: Outlet[KvdbBatch] = Outlet[KvdbBatch]("FdbAsyncIteratorToSourceStage.out")
  override val shape: SourceShape[KvdbBatch] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = shutdownSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with GraphStageWithActorLogic {
      private lazy val batchEmitter = new BatchEmitter(
        actor = self,
        iterator = iterator,
        maxBatchBytes = maxBatchBytes,
        keyValidator = keyValidator,
        keyTransformer = keyTransformer,
        emit = emit(out, _)
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            self ! DownstreamPull
          }
          override def onDownstreamFinish(cause: Throwable): Unit = {
            self ! DownstreamFinish
            super.onDownstreamFinish(cause)
          }
        }
      )

      override def preStart(): Unit = {
        val _ = getStageActor(makeHandler({
          case DownstreamPull =>
            val _ = batchEmitter.batchAndEmit(None)
          case DownstreamFinish | IteratorComplete =>
            completeStage()
          case IteratorNext(kv) =>
            val _ = batchEmitter.batchAndEmit(Some(kv.getKey -> kv.getValue))
          case IteratorFailure(ex) =>
            failStage(ex)
        }))
      }

      override def postStop(): Unit = {
        try {
          close()
        }
        finally {
          shutdownListener.unregister()
          super.postStop()
        }
      }
    }
  }
}
