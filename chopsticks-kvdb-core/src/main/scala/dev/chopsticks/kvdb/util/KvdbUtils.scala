package dev.chopsticks.kvdb.util

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Cancellable
import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.{Done, NotUsed}
import com.google.protobuf.{ByteString => ProtoByteString}
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange, KvdbOperationException}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.util.control.NoStackTrace

object KvdbUtils {

  final case class KvdbClientOptions(maxBatchBytes: Int, tailPollingInterval: FiniteDuration)

  object Implicits {
    implicit val defaultClientOptions: KvdbClientOptions =
      KvdbClientOptions(maxBatchBytes = 32 * 1024, tailPollingInterval = 100.millis)
  }

  final case class EmptyTail(time: Instant, lastKey: Option[Array[Byte]])

  type KvdbPair = (Array[Byte], Array[Byte])
  type KvdbBatch = Array[KvdbPair]
  type KvdbValueBatch = Array[Array[Byte]]
  type KvdbTailBatch = Either[EmptyTail, KvdbBatch]
  type KvdbTailValueBatch = Either[EmptyTail, KvdbValueBatch]
  type KvdbIndexedTailBatch = (Int, KvdbTailBatch)

  sealed abstract class KvdbException(val exceptionType: KvdbOperationException.ExceptionType, msg: String)
      extends RuntimeException(msg)
      with NoStackTrace

  final case class SeekFailure(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.SEEK_FAILURE, msg)

  final case class UnoptimizedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNOPTIMIZED_OPERATION, msg)

  final case class InvalidKvdbColumnFamilyException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_COLUMN_FAMILY, msg)

  final case class InvalidKvdbArgumentException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.INVALID_ARGUMENT, msg)

  final case class UnsupportedKvdbOperationException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.UNSUPPORTED_OPERATION, msg)

  final case class KvdbAlreadyClosedException(msg: String)
      extends KvdbException(KvdbOperationException.ExceptionType.ALREADY_CLOSED, msg)

  final class KvdbCloseSignal {
    final class Listener private[KvdbCloseSignal] {
      private[KvdbCloseSignal] val promise = Promise[Done]
      def future: Future[Done] = promise.future
      def unregister(): Unit = removeListener(this)
    }

    private[this] val _listeners = TrieMap.empty[Listener, NotUsed]
    private[this] val _completedWith: AtomicReference[Option[Try[Done]]] = new AtomicReference(None)

    def tryComplete(result: Try[Done]): Unit = {
      if (_completedWith.compareAndSet(None, Some(result))) {
        for ((listener, _) ← _listeners) listener.promise.tryComplete(result)
      }
    }

    def createListener(): Listener = {
      val listener = new Listener
      if (_completedWith.get.isEmpty) {
        val _ = _listeners += (listener → NotUsed)
      }
      _completedWith.get match {
        case Some(result) => val _ = listener.promise.tryComplete(result)
        case None => // Ignore.
      }
      listener
    }

    def hasNoListeners: Boolean = _listeners.isEmpty

    private def removeListener(listener: Listener): Unit = {
      val _ = _listeners -= listener
    }
  }

  final case class KvdbIterateSourceGraphRefs(iterator: Iterator[KvdbPair], close: () => Unit)

  class KvdbIterateSourceGraph(
    createRefs: () => Either[Exception, KvdbIterateSourceGraphRefs],
    closeSignal: KvdbCloseSignal,
    dispatcher: String
  )(implicit clientOptions: KvdbClientOptions)
      extends GraphStage[SourceShape[KvdbBatch]] {
    val outlet: Outlet[KvdbBatch] = Outlet("KvdbIterateSourceGraph.out")

    override protected def initialAttributes = ActorAttributes.dispatcher(dispatcher)

    val shape: SourceShape[KvdbBatch] = SourceShape[KvdbBatch](outlet)

    def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      val shutdownListener = closeSignal.createListener()

      new KillableGraphStageLogic(shutdownListener.future, shape) with StageLogging {
        var refs: KvdbIterateSourceGraphRefs = _

        private def canContinue: Boolean = {
          // scalastyle:off null
          if (refs eq null) {
            // scalastyle:on null
            createRefs() match {
              case Left(ex) =>
                failStage(ex)
                false

              case Right(r) =>
                refs = r
                true
            }
          }
          else true
        }

        private val reusableBuffer: mutable.ArrayBuilder[(Array[Byte], Array[Byte])] = {
          val b = Array.newBuilder[KvdbPair]
          b.sizeHint(1000)
          b
        }

        setHandler(
          outlet,
          new OutHandler {
            def onPull(): Unit = {
              if (canContinue) {
                var batchSizeSoFar = 0
                val iterator = refs.iterator
                var isEmpty = true

                while (batchSizeSoFar < clientOptions.maxBatchBytes && iterator.hasNext) {
                  val next = iterator.next()
                  batchSizeSoFar += next._1.length + next._2.length
                  val _ = reusableBuffer += next
                  isEmpty = false
                }

                if (isEmpty) {
                  completeStage()
                }
                else {
                  val ret = reusableBuffer.result()
                  reusableBuffer.clear()
                  emit(outlet, ret)
                }
              }
            }

            override def onDownstreamFinish(): Unit = {
              completeStage()
              super.onDownstreamFinish()
            }
          }
        )

        override def postStop(): Unit = {
          try {
            // scalastyle:off null
            if (refs ne null) {
              refs.close()
            }
            // scalastyle:on null
          } finally {
            shutdownListener.unregister()
            super.postStop()
          }
        }
      }
    }
  }

  final case class KvdbTailSourceGraphRefs(
    createIterator: () => Iterator[KvdbPair],
    clear: () => Unit,
    close: () => Unit
  )

  class KvdbTailSourceGraph(
    init: () => KvdbTailSourceGraphRefs,
    closeSignal: KvdbCloseSignal,
    dispatcher: String,
    pollingBackoffFactor: Double Refined Positive = 1.15d
  )(implicit clientOptions: KvdbClientOptions)
      extends GraphStage[SourceShape[KvdbTailBatch]] {
    val outlet: Outlet[KvdbTailBatch] = Outlet("KvdbIterateSourceGraph.out")

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
        private var _refs: KvdbTailSourceGraphRefs = _

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

            while (batchSizeSoFar < clientOptions.maxBatchBytes && iterator.hasNext) {
              val next = iterator.next()
              batchSizeSoFar += next._1.length + next._2.length
              val _ = reusableBuffer += next
              isEmpty = false
            }

            if (isEmpty) {
              pollingDelay = {
                if (pollingDelay.length == 0) 1.milli
                else if (pollingDelay < clientOptions.tailPollingInterval)
                  Duration.fromNanos((pollingDelay.toNanos * pollingBackoffFactor).toLong)
                else clientOptions.tailPollingInterval
              }

              clearIterator()

              val emptyTail = EmptyTail(Instant.now, lastKey)

              timer = materializer.scheduleOnce(pollingDelay, () => {
                timerAsyncCallback.invoke(emptyTail)
              })
            }
            else {
              pollingDelay = Duration.Zero
              val ret = reusableBuffer.result()
              reusableBuffer.clear()
              lastKey = Some(ret.last._1)

              emit(outlet, Right(ret))
            }
          }

          override def onDownstreamFinish(): Unit = {
            completeStage()
            super.onDownstreamFinish()
          }
        }

        setHandler(outlet, outHandler)

        override def postStop(): Unit = {
          try {
            cleanUp()
          } finally {
            shutdownListener.unregister()
            super.postStop()
          }
        }
      }
    }
  }

  def resumeRange(range: KvdbKeyRange, lastKey: Array[Byte], lastKeyDisplay: String): KvdbKeyRange = {
    if (lastKey.isEmpty) range
    else {
      val rangeTo = range.to
      val tail = if (rangeTo.size == 1 && rangeTo.head.operator == Operator.LAST) Nil else rangeTo
      range.copy(from = KvdbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey), lastKeyDisplay) :: tail)
    }
  }

//  private val STRING_MAX = new String(List[Byte](Byte.MaxValue).toArray, "UTF8")

//  def stringPrefixKvdbKeyRange(value: String): KvdbKeyRange = {
//    val end = value + STRING_MAX
//    KvdbKeyConstraints.range[String](_ >= value < end, _ < end)
//  }
}
