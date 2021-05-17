/* Forked and modified from https://github.com/akka/akka/blob/2dde4b6b512d10d1db37764ab13e46cea8f49429/akka-stream/src/main/scala/akka/stream/scaladsl/Hub.scala */
package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.{Attributes, Inlet, Outlet, SinkShape, SourceShape, StreamDetachedException}
import akka.stream.scaladsl.Source
import akka.stream.stage.{
  AsyncCallback,
  GraphStage,
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler
}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

object ReplayLastBroadcastHub {
  def apply[T](bufferSize: Int): ReplayLastBroadcastHub[T] = new ReplayLastBroadcastHub(bufferSize)
}

final class ReplayLastBroadcastHub[T] private (bufferSize: Int)
    extends GraphStageWithMaterializedValue[SinkShape[T], Source[T, NotUsed]] {
  require(bufferSize > 0, "Buffer size must be positive")
  require(bufferSize < 4096, "Buffer size larger then 4095 is not allowed")
  require((bufferSize & bufferSize - 1) == 0, "Buffer size must be a power of two")

  private val Mask = bufferSize - 1
  private val WheelMask = (bufferSize * 2) - 1

  val in: Inlet[T] = Inlet("BroadcastHub.in")
  override val shape: SinkShape[T] = SinkShape(in)

  // Half of buffer size, rounded up
  private[this] val DemandThreshold = (bufferSize / 2) + (bufferSize % 2)

  sealed private trait HubEvent

  private object RegistrationPending extends HubEvent
  // these 4 next classes can't be final because of SI-4440
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class UnRegister(id: Long, previousOffset: Int, finalOffset: Int) extends HubEvent
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class Advance(id: Long, previousOffset: Int) extends HubEvent
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class NeedWakeup(id: Long, previousOffset: Int, currentOffset: Int) extends HubEvent
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class Consumer(id: Long, callback: AsyncCallback[ConsumerEvent])

  private object Completed

  sealed private trait HubState
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class Open(callbackFuture: Future[AsyncCallback[HubEvent]], registrations: List[Consumer])
      extends HubState
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class Closed(failure: Option[Throwable]) extends HubState

  private class BroadcastSinkLogic(_shape: Shape) extends GraphStageLogic(_shape) with InHandler {

    private[this] val callbackPromise: Promise[AsyncCallback[HubEvent]] = Promise()
    private[this] val noRegistrationsState = Open(callbackPromise.future, Nil)
    val state = new AtomicReference[HubState](noRegistrationsState)

    // Start from values that will almost immediately overflow. This has no effect on performance, any starting
    // number will do, however, this protects from regressions as these values *almost surely* overflow and fail
    // tests if someone makes a mistake.
    @volatile private[this] var tail = Int.MaxValue
    private[this] var head = Int.MaxValue
    /*
     * An Array with a published tail ("latest message") and a privately maintained head ("earliest buffered message").
     * Elements are published by simply putting them into the array and bumping the tail. If necessary, certain
     * consumers are sent a wakeup message through an AsyncCallback.
     */
    private[this] val queue = new Array[AnyRef](bufferSize)
    /* This is basically a classic Bucket Queue: https://en.wikipedia.org/wiki/Bucket_queue
     * (in fact, this is the variant described in the Optimizations section, where the given set
     * of priorities always fall to a range
     *
     * This wheel tracks the position of Consumers relative to the slowest ones. Every slot
     * contains a list of Consumers being known at that location (this might be out of date!).
     * Consumers from time to time send Advance messages to indicate that they have progressed
     * by reading from the broadcast queue. Consumers that are blocked (due to reaching tail) request
     * a wakeup and update their position at the same time.
     *
     */
    private[this] val consumerWheel = Array.fill[List[Consumer]](bufferSize * 2)(Nil)
    private[this] var activeConsumers = 0

    private[this] var lastElement: T = null.asInstanceOf[T]

    override def preStart(): Unit = {
      setKeepGoing(true)
      val _ = callbackPromise.success(getAsyncCallback[HubEvent](onEvent))
      pull(in)
    }

    // Cannot complete immediately if there is no space in the queue to put the completion marker
    override def onUpstreamFinish(): Unit = if (!isFull) complete()

    override def onPush(): Unit = {
      publish(grab(in))
      if (!isFull) pull(in)
    }

    private def onEvent(ev: HubEvent): Unit = {
      ev match {
        case RegistrationPending =>
          state.getAndSet(noRegistrationsState).asInstanceOf[Open].registrations.foreach { consumer =>
            val startFrom = head
            activeConsumers += 1
            addConsumer(consumer, startFrom)
            // in case the consumer is already stopped we need to undo registration
            implicit val ec: ExecutionContextExecutor = materializer.executionContext
            consumer.callback.invokeWithFeedback(Initialize(startFrom, lastElement)).failed.foreach {
              case _: StreamDetachedException =>
                callbackPromise.future.foreach(callback =>
                  callback.invoke(UnRegister(consumer.id, startFrom, startFrom))
                )
              case _ => ()
            }
          }

        case UnRegister(id, previousOffset, finalOffset) =>
          if (findAndRemoveConsumer(id, previousOffset) != null)
            activeConsumers -= 1
          if (activeConsumers == 0) {
            if (isClosed(in)) completeStage()
            else if (head != finalOffset) {
              // If our final consumer goes away, we roll forward the buffer so a subsequent consumer does not
              // see the already consumed elements. This feature is quite handy.
              while (head != finalOffset) {
                queue(head & Mask) = null
                head += 1
              }
              head = finalOffset
              if (!hasBeenPulled(in)) pull(in)
            }
          }
          else checkUnblock(previousOffset)

        case Advance(id, previousOffset) =>
          val newOffset = previousOffset + DemandThreshold
          // Move the consumer from its last known offset to its new one. Check if we are unblocked.
          val consumer = findAndRemoveConsumer(id, previousOffset)
          addConsumer(consumer, newOffset)
          checkUnblock(previousOffset)
        case NeedWakeup(id, previousOffset, currentOffset) =>
          // Move the consumer from its last known offset to its new one. Check if we are unblocked.
          val consumer = findAndRemoveConsumer(id, previousOffset)
          addConsumer(consumer, currentOffset)

          // Also check if the consumer is now unblocked since we published an element since it went asleep.
          if (currentOffset != tail) consumer.callback.invoke(Wakeup)
          checkUnblock(previousOffset)
      }
    }

    // Producer API
    // We are full if the distance between the slowest (known) consumer and the fastest (known) consumer is
    // the buffer size. We must wait until the slowest either advances, or cancels.
    private def isFull: Boolean = tail - head == bufferSize

    override def onUpstreamFailure(ex: Throwable): Unit = {
      val failMessage = HubCompleted(Some(ex))

      // Notify pending consumers and set tombstone
      state.getAndSet(Closed(Some(ex))).asInstanceOf[Open].registrations.foreach { consumer =>
        consumer.callback.invoke(failMessage)
      }

      // Notify registered consumers
      consumerWheel.iterator.flatMap(_.iterator).foreach { consumer =>
        consumer.callback.invoke(failMessage)
      }
      failStage(ex)
    }

    /*
     * This method removes a consumer with a given ID from the known offset and returns it.
     *
     * NB: You cannot remove a consumer without knowing its last offset! Consumers on the Source side always must
     * track this so this can be a fast operation.
     */
    private def findAndRemoveConsumer(id: Long, offset: Int): Consumer = {
      // TODO: Try to eliminate modulo division somehow...
      val wheelSlot = offset & WheelMask
      var consumersInSlot = consumerWheel(wheelSlot)
      //debug(s"consumers before removal $consumersInSlot")
      var remainingConsumersInSlot: List[Consumer] = Nil
      var removedConsumer: Consumer = null

      while (consumersInSlot.nonEmpty) {
        val consumer = consumersInSlot.head
        if (consumer.id != id) remainingConsumersInSlot = consumer :: remainingConsumersInSlot
        else removedConsumer = consumer
        consumersInSlot = consumersInSlot.tail
      }
      consumerWheel(wheelSlot) = remainingConsumersInSlot
      removedConsumer
    }

    /*
     * After removing a Consumer from a wheel slot (because it cancelled, or we moved it because it advanced)
     * we need to check if it was blocking us from advancing (being the slowest).
     */
    private def checkUnblock(offsetOfConsumerRemoved: Int): Unit = {
      if (unblockIfPossible(offsetOfConsumerRemoved)) {
        if (isClosed(in)) complete()
        else if (!hasBeenPulled(in)) pull(in)
      }
    }

    private def unblockIfPossible(offsetOfConsumerRemoved: Int): Boolean = {
      var unblocked = false
      if (offsetOfConsumerRemoved == head) {
        // Try to advance along the wheel. We can skip any wheel slots which have no waiting Consumers, until
        // we either find a nonempty one, or we reached the end of the buffer.
        while (consumerWheel(head & WheelMask).isEmpty && head != tail) {
          queue(head & Mask) = null
          head += 1
          unblocked = true
        }
      }
      unblocked
    }

    private def addConsumer(consumer: Consumer, offset: Int): Unit = {
      val slot = offset & WheelMask
      consumerWheel(slot) = consumer :: consumerWheel(slot)
    }

    /*
     * Send a wakeup signal to all the Consumers at a certain wheel index. Note, this needs the actual index,
     * which is offset modulo (bufferSize + 1).
     */
    private def wakeupIdx(idx: Int): Unit = {
      val itr = consumerWheel(idx).iterator
      while (itr.hasNext) itr.next().callback.invoke(Wakeup)
    }

    private def complete(): Unit = {
      val idx = tail & Mask
      val wheelSlot = tail & WheelMask
      queue(idx) = Completed
      wakeupIdx(wheelSlot)
      tail = tail + 1
      if (activeConsumers == 0) {
        // Existing consumers have already consumed all elements and will see completion status in the queue
        completeStage()
      }
    }

    override def postStop(): Unit = {
      // Notify pending consumers and set tombstone

      @tailrec def tryClose(): Unit = state.get() match {
        case Closed(_) => // Already closed, ignore
        case open: Open =>
          if (state.compareAndSet(open, Closed(None))) {
            val completedMessage = HubCompleted(None)
            open.registrations.foreach { consumer =>
              consumer.callback.invoke(completedMessage)
            }
          }
          else tryClose()
      }

      tryClose()
    }

    private def publish(elem: T): Unit = {
      val idx = tail & Mask
      val wheelSlot = tail & WheelMask
      queue(idx) = elem.asInstanceOf[AnyRef]
      // Publish the new tail before calling the wakeup
      tail = tail + 1
      wakeupIdx(wheelSlot)
      lastElement = elem
    }

    // Consumer API
    def poll(offset: Int): AnyRef = {
      if (offset == tail) null
      else queue(offset & Mask)
    }

    setHandler(in, this)

  }

  sealed private trait ConsumerEvent
  private object Wakeup extends ConsumerEvent
  // these two can't be final because of SI-4440
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class HubCompleted(failure: Option[Throwable]) extends ConsumerEvent
  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private case class Initialize(offset: Int, lastElement: T) extends ConsumerEvent

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes
  ): (GraphStageLogic, Source[T, NotUsed]) = {
    val idCounter = new AtomicLong()

    val logic = new BroadcastSinkLogic(shape)

    val source = new GraphStage[SourceShape[T]] {
      val out: Outlet[T] = Outlet("BroadcastHub.out")
      override val shape: SourceShape[T] = SourceShape(out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with OutHandler {
          private[this] var untilNextAdvanceSignal = DemandThreshold
          private[this] val id = idCounter.getAndIncrement()
          private[this] var offsetInitialized = false
          private[this] var hubCallback: AsyncCallback[HubEvent] = _

          /*
           * We need to track our last offset that we published to the Hub. The reason is, that for efficiency reasons,
           * the Hub can only look up and move/remove Consumers with known wheel slots. This means that no extra hash-map
           * is needed, but it also means that we need to keep track of both our current offset, and the last one that
           * we published.
           */
          private[this] var previousPublishedOffset = 0
          private[this] var offset = 0
          private[this] var initialElement: T = null.asInstanceOf[T]
          private[this] var initialElementHandled: Boolean = false

          override def preStart(): Unit = {
            val callback = getAsyncCallback(onCommand)

            val onHubReady: Try[AsyncCallback[HubEvent]] => Unit = {
              case Success(callback) =>
                hubCallback = callback
                if (isAvailable(out) && offsetInitialized) onPull()
                callback.invoke(RegistrationPending)
              case Failure(ex) =>
                failStage(ex)
            }

            @tailrec def register(): Unit = {
              logic.state.get() match {
                case Closed(Some(ex)) => failStage(ex)
                case Closed(None) => completeStage()
                case previousState @ Open(callbackFuture, registrations) =>
                  val newRegistrations = Consumer(id, callback) :: registrations
                  if (logic.state.compareAndSet(previousState, Open(callbackFuture, newRegistrations))) {
                    callbackFuture.onComplete(getAsyncCallback(onHubReady).invoke)(materializer.executionContext)
                  }
                  else register()
              }
            }

            /*
             * Note that there is a potential race here. First we add ourselves to the pending registrations, then
             * we send RegistrationPending. However, another downstream might have triggered our registration by its
             * own RegistrationPending message, since we are in the list already.
             * This means we might receive an onCommand(Initialize(offset)) *before* onHubReady fires so it is important
             * to only serve elements after both offsetInitialized = true and hubCallback is not null.
             */
            register()

          }

          override def onPull(): Unit = {
            if (offsetInitialized && (hubCallback ne null)) {
              val elem = logic.poll(offset)

              elem match {
                case null =>
                  if (!initialElementHandled) {
                    initialElementHandled = true
                    if (initialElement != null) {
                      push(out, initialElement)
                    }
                  }

                  hubCallback.invoke(NeedWakeup(id, previousPublishedOffset, offset))
                  previousPublishedOffset = offset
                  untilNextAdvanceSignal = DemandThreshold
                case Completed =>
                  completeStage()
                case _ =>
                  initialElementHandled = true
                  push(out, elem.asInstanceOf[T])
                  offset += 1
                  untilNextAdvanceSignal -= 1
                  if (untilNextAdvanceSignal == 0) {
                    untilNextAdvanceSignal = DemandThreshold
                    val previousOffset = previousPublishedOffset
                    previousPublishedOffset += DemandThreshold
                    hubCallback.invoke(Advance(id, previousOffset))
                  }
              }

            }
          }

          override def postStop(): Unit = {
            if (hubCallback ne null)
              hubCallback.invoke(UnRegister(id, previousPublishedOffset, offset))
          }

          private def onCommand(cmd: ConsumerEvent): Unit = cmd match {
            case HubCompleted(Some(ex)) => failStage(ex)
            case HubCompleted(None) => completeStage()
            case Wakeup =>
              if (isAvailable(out)) onPull()
            case Initialize(initialOffset, lastElement) =>
              initialElement = lastElement
              offsetInitialized = true
              previousPublishedOffset = initialOffset
              offset = initialOffset
              if (isAvailable(out) && (hubCallback ne null)) onPull()
          }

          setHandler(out, this)
        }
    }

    (logic, Source.fromGraph(source))
  }
}
