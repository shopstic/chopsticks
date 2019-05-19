package dev.chopsticks.util

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Cancellable
import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage._
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.{Done, NotUsed}
import com.google.protobuf.{ByteString => ProtoByteString}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric._
import dev.chopsticks.codec.DbKeyConstraints
import dev.chopsticks.proto.db.DbKeyConstraint.Operator
import dev.chopsticks.proto.db.{DbKeyConstraint, DbKeyRange, DbOperationException}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.util.control.NoStackTrace

object DbUtils {

  final case class DbClientOptions(maxBatchBytes: Int, tailPollingInterval: FiniteDuration)

  object Implicits {
    implicit val defaultDbClientOptions: DbClientOptions =
      DbClientOptions(maxBatchBytes = 32 * 1024, tailPollingInterval = 100.millis)
  }

  final case class EmptyTail(time: Instant, lastKey: Option[Array[Byte]])

  type DbPair = (Array[Byte], Array[Byte])
  type DbBatch = Array[DbPair]
  type DbValueBatch = Array[Array[Byte]]
  type DbTailBatch = Either[EmptyTail, DbBatch]
  type DbTailValueBatch = Either[EmptyTail, DbValueBatch]
  type DbIndexedTailBatch = (Int, DbTailBatch)

  sealed abstract class DbException(val exceptionType: DbOperationException.ExceptionType, msg: String)
      extends RuntimeException(msg)
      with NoStackTrace

  final case class SeekFailure(msg: String) extends DbException(DbOperationException.ExceptionType.SEEK_FAILURE, msg)

  final case class UnoptimizedDbOperationException(msg: String)
      extends DbException(DbOperationException.ExceptionType.UNOPTIMIZED_OPERATION, msg)

  final case class InvalidDbColumnFamilyException(msg: String)
      extends DbException(DbOperationException.ExceptionType.INVALID_COLUMN_FAMILY, msg)

  final case class InvalidDbArgumentException(msg: String)
      extends DbException(DbOperationException.ExceptionType.INVALID_ARGUMENT, msg)

  final case class UnsupportedDbOperationException(msg: String)
      extends DbException(DbOperationException.ExceptionType.UNSUPPORTED_OPERATION, msg)

  final case class DbAlreadyClosedException(msg: String)
      extends DbException(DbOperationException.ExceptionType.ALREADY_CLOSED, msg)

  final class DbCloseSignal {
    final class Listener private[DbCloseSignal] {
      private[DbCloseSignal] val promise = Promise[Done]
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

  final case class DbIterateSourceGraphRefs(iterator: Iterator[DbPair], close: () => Unit)

  class DbIterateSourceGraph(
    createRefs: () => Either[Exception, DbIterateSourceGraphRefs],
    closeSignal: DbCloseSignal,
    dispatcher: String
  )(implicit clientOptions: DbClientOptions)
      extends GraphStage[SourceShape[DbBatch]] {
    val outlet: Outlet[DbBatch] = Outlet("DbIterateSourceGraph.out")

    override protected def initialAttributes = ActorAttributes.dispatcher(dispatcher)

    val shape: SourceShape[DbBatch] = SourceShape[DbBatch](outlet)

    def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      val shutdownListener = closeSignal.createListener()

      new KillableGraphStageLogic(shutdownListener.future, shape) with StageLogging {
        var refs: DbIterateSourceGraphRefs = _

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
          val b = Array.newBuilder[DbPair]
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

  final case class DbTailSourceGraphRefs(createIterator: () => Iterator[DbPair], clear: () => Unit, close: () => Unit)

  class DbTailSourceGraph(
    init: () => DbTailSourceGraphRefs,
    closeSignal: DbCloseSignal,
    dispatcher: String,
    pollingBackoffFactor: Double Refined Positive = 1.15d
  )(implicit clientOptions: DbClientOptions)
      extends GraphStage[SourceShape[DbTailBatch]] {
    val outlet: Outlet[DbTailBatch] = Outlet("DbIterateSourceGraph.out")

    override protected def initialAttributes = ActorAttributes.dispatcher(dispatcher)

    val shape: SourceShape[DbTailBatch] = SourceShape[DbTailBatch](outlet)

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

        private var _iterator: Iterator[DbPair] = _
        private var _refs: DbTailSourceGraphRefs = _

        private def clearIterator(): Unit = {
          // scalastyle:off null
          _iterator = null
          if (_refs ne null) _refs.clear()
          // scalastyle:on null
        }

        // scalastyle:off null
        private def iterator: Iterator[DbPair] = {
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
          val b = Array.newBuilder[DbPair]
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

  def resumeRange(range: DbKeyRange, lastKey: Array[Byte], lastKeyDisplay: String): DbKeyRange = {
    if (lastKey.isEmpty) range
    else {
      val rangeTo = range.to
      val tail = if (rangeTo.size == 1 && rangeTo.head.operator == Operator.LAST) Nil else rangeTo
      range.copy(from = DbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey), lastKeyDisplay) :: tail)
    }
  }

  private val STRING_MAX = new String(List[Byte](Byte.MaxValue).toArray, "UTF8")

  def stringPrefixDbKeyRange(value: String): DbKeyRange = {
    val end = value + STRING_MAX
    DbKeyConstraints.range[String](_ >= value < end, _ < end)
  }
}
