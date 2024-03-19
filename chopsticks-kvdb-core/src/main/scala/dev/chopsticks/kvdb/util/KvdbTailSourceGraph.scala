package dev.chopsticks.kvdb.util

import java.time.Instant

import org.apache.pekko.actor.Cancellable
import org.apache.pekko.stream.KillSwitches.KillableGraphStageLogic
import org.apache.pekko.stream.stage._
import org.apache.pekko.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbPair, KvdbTailBatch}
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import squants.information.Information

import scala.collection.mutable
import scala.concurrent.duration._

object KvdbTailSourceGraph {
  final case class EmptyTail(time: Instant, lastKey: Option[Array[Byte]])

  final case class Refs(
    createIterator: () => Iterator[KvdbPair],
    clear: () => Unit,
    close: () => Unit
  )
}

class KvdbTailSourceGraph(
  init: () => KvdbTailSourceGraph.Refs,
  closeSignal: KvdbCloseSignal,
  dispatcher: String,
  maxBatchBytes: Information,
  tailPollingInterval: FiniteDuration,
  tailPollingBackoffFactor: Double Refined Greater[W.`1.0d`.T] = 1.15d
) extends GraphStage[SourceShape[KvdbTailBatch]] {
  import KvdbTailSourceGraph._
  val outlet: Outlet[KvdbTailBatch] = Outlet("KvdbIterateSourceGraph.out")
  private val maxBatchBytesInt = maxBatchBytes.toBytes.toInt

  override protected def initialAttributes = ActorAttributes.dispatcher(dispatcher)

  val shape: SourceShape[KvdbTailBatch] = SourceShape[KvdbTailBatch](outlet)

  // scalastyle:off method.length
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = closeSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with StageLogging {
      private var pollingDelay: FiniteDuration = Duration.Zero
      private var timer: Cancellable = _
      private var _timerAsyncCallback: AsyncCallback[EmptyTail] = _
      private def timerAsyncCallback: AsyncCallback[EmptyTail] = {
        // scalastyle:off null
        if (_timerAsyncCallback eq null) {
          _timerAsyncCallback = getAsyncCallback[EmptyTail] { e =>
            timer = null
            emit(outlet, Left(e))
          }
        }
        // scalastyle:on null
        _timerAsyncCallback
      }

      private var _iterator: Iterator[KvdbPair] = _
      private var _refs: Refs = _

      private def clearIterator(): Unit = {
        // scalastyle:off null
        _iterator = null
        if (_refs ne null) _refs.clear()
        // scalastyle:on null
      }

      // scalastyle:off null
      private def iterator: Iterator[KvdbPair] = {
        if (_refs eq null) _refs = init()
        if (_iterator eq null) _iterator = _refs.createIterator()
        _iterator
      }
      // scalastyle:on null

      private def cleanUp(): Unit = {
        // scalastyle:off null
        if (timer ne null) {
          val _ = timer.cancel()
        }
        if (_refs ne null) _refs.close()
        // scalastyle:on null
      }

      private var lastKey: Option[Array[Byte]] = None

      private val reusableBuffer: mutable.ArrayBuilder[(Array[Byte], Array[Byte])] = {
        val b = Array.newBuilder[KvdbPair]
        b.sizeHint(1000)
        b
      }

      private val outHandler = new OutHandler {
        def onPull(): Unit = {
          var batchSizeSoFar = 0
          var isEmpty = true

          while (batchSizeSoFar < maxBatchBytesInt && iterator.hasNext) {
            val next = iterator.next()
            batchSizeSoFar += next._1.length + next._2.length
            val _ = reusableBuffer += next
            isEmpty = false
          }

          if (isEmpty) {
            pollingDelay = {
              if (pollingDelay.length == 0) 1.milli
              else if (pollingDelay < tailPollingInterval)
                Duration.fromNanos((pollingDelay.toNanos * tailPollingBackoffFactor).toLong)
              else tailPollingInterval
            }

            clearIterator()

            val emptyTail = EmptyTail(Instant.now, lastKey)

            timer = materializer.scheduleOnce(
              pollingDelay,
              () => {
                timerAsyncCallback.invoke(emptyTail)
              }
            )
          }
          else {
            pollingDelay = Duration.Zero
            val ret = reusableBuffer.result()
            reusableBuffer.clear()
            lastKey = Some(ret.last._1)

            emit(outlet, Right(ret))
          }
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          completeStage()
          super.onDownstreamFinish(cause)
        }
      }

      setHandler(outlet, outHandler)

      override def postStop(): Unit = {
        try {
          cleanUp()
        }
        finally {
          shutdownListener.unregister()
          super.postStop()
        }
      }
    }
  }
}
