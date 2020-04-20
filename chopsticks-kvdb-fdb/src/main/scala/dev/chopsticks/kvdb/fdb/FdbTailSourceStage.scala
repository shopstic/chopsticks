package dev.chopsticks.kvdb.fdb

import java.time.Instant

import akka.actor.Cancellable
import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.async.AsyncIterator
import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbTailBatch
import dev.chopsticks.kvdb.util.KvdbTailSourceGraph.EmptyTail
import dev.chopsticks.kvdb.util.{KvdbClientOptions, KvdbCloseSignal}
import dev.chopsticks.stream.GraphStageWithActorLogic
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.auto._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.duration._

object FdbTailSourceStage {
  final private[fdb] case class EmitEmptyTail(emptyTail: EmptyTail)
}

final class FdbTailSourceStage(
  initialRange: KvdbKeyRange,
  iterate: KvdbKeyRange => (AsyncIterator[KeyValue], () => Unit),
  keyValidator: Array[Byte] => Boolean,
  keyTransformer: Array[Byte] => Array[Byte],
  shutdownSignal: KvdbCloseSignal,
  pollingBackoffFactor: Double Refined Positive = 1.15d
)(implicit clientOptions: KvdbClientOptions)
    extends GraphStage[SourceShape[KvdbTailBatch]] {
  import FdbIterateSourceStage._
  import FdbTailSourceStage._

  private val maxBatchBytes = clientOptions.maxBatchBytes.toBytes.toInt
  private val out: Outlet[KvdbTailBatch] = Outlet[KvdbTailBatch]("FdbAsyncIteratorToSourceStage.out")
  override val shape: SourceShape[KvdbTailBatch] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = shutdownSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with GraphStageWithActorLogic {
      private var timer = Option.empty[Cancellable]
      private var pollingDelay: FiniteDuration = Duration.Zero

      private def pendingEmitEmptyTail(newRange: KvdbKeyRange): Receive = {
        case DownstreamPull => // Ignore
        case DownstreamFinish =>
          timer.foreach(_.cancel())
          completeStage()
        case EmitEmptyTail(emptyTail) =>
          timer = None
          stageActor.become(makeHandler(pendingPull(newRange)))
          emit(out, Left(emptyTail))
      }

      private def pendingPull(range: KvdbKeyRange): Receive = {
        case DownstreamPull =>
          val (iterator, close) = iterate(range)
          val batchEmitter = new BatchEmitter(
            actor = self,
            iterator = iterator,
            maxBatchBytes = maxBatchBytes,
            keyValidator = keyValidator,
            keyTransformer = keyTransformer,
            emit = batch => {
              emit(out, Right(batch))
            }
          )
          stageActor.become(makeHandler(pulling(batchEmitter, range, close)))
          val _ = batchEmitter.batchAndEmit(None)

        case DownstreamFinish =>
          completeStage()
      }

      private def pulling(batchEmitter: BatchEmitter, range: KvdbKeyRange, close: () => Unit): Receive = {
        case DownstreamPull =>
          if (batchEmitter.batchAndEmit(None) > 0) {
            pollingDelay = Duration.Zero
          }

        case IteratorComplete =>
          close()
          val maybeLastKey = batchEmitter.maybeLastKey
          val newRange = range.withFrom(
            maybeLastKey.fold(range.from) { lastKey =>
              KvdbKeyConstraint(Operator.GREATER, ByteString.copyFrom(lastKey)) :: range.from.tail
            }
          )

          stageActor.become(makeHandler(pendingEmitEmptyTail(newRange)))

          pollingDelay = {
            if (pollingDelay.length == 0) 1.milli
            else if (pollingDelay < clientOptions.tailPollingInterval)
              Duration.fromNanos((pollingDelay.toNanos * pollingBackoffFactor).toLong)
            else clientOptions.tailPollingInterval
          }

          val emptyTail = EmptyTail(Instant.now, maybeLastKey)
          timer = Some(materializer.scheduleOnce(pollingDelay, () => {
            self ! EmitEmptyTail(emptyTail)
          }))

        case IteratorNext(kv) =>
          if (batchEmitter.batchAndEmit(Some(kv.getKey -> kv.getValue)) > 0) {
            pollingDelay = Duration.Zero
          }

        case IteratorFailure(ex) =>
          close()
          log.info(s"Got ${ex.getMessage}, will retry")
          self ! IteratorComplete

        case DownstreamFinish =>
          close()
          completeStage()
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            self ! DownstreamPull
          }
          override def onDownstreamFinish(cause: Throwable): Unit = {
            setKeepGoing(true)
            self ! DownstreamFinish
          }
        }
      )

      override def postStop(): Unit = {
        super.postStop()
        timer.foreach(_.cancel())
      }

      override def preStart(): Unit = {
        val _ = getStageActor(makeHandler(pendingPull(initialRange)))
      }
    }
  }
}
