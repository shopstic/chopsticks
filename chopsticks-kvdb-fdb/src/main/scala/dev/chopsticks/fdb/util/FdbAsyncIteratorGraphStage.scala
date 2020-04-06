package dev.chopsticks.fdb.util

import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.async.AsyncIterator

import scala.concurrent.ExecutionContextExecutor
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success}

final class FdbAsyncIteratorGraphStage[V](create: => AsyncIterator[V]) extends GraphStage[SourceShape[V]] {
  private val out: Outlet[V] = Outlet[V]("FdbAsyncIteratorGraphStage.out")
  override val shape: SourceShape[V] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var currentIterator = Option.empty[AsyncIterator[V]]

      setHandler(out, eagerTerminateOutput)

      private def closeIterator(): Unit = {
        currentIterator = currentIterator match {
          case Some(it) =>
            it.cancel()
            None
          case None =>
            None
        }
      }

      override def postStop(): Unit = {
        super.postStop()
        closeIterator()
      }

      override def preStart(): Unit = {
        implicit val ec: ExecutionContextExecutor = materializer.executionContext
        val onIteratorComplete: AsyncCallback[Unit] = getAsyncCallback[Unit] { (_) => completeStage() }
        val onIteratorNext: AsyncCallback[V] = getAsyncCallback[V] { kv => emit(out, kv) }
        val onIteratorFailure: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { e => failStage(e) }
        val iterator = create
        currentIterator = Some(iterator)

        setHandler(
          out,
          new OutHandler {
            override def onDownstreamFinish(cause: Throwable): Unit = {
              super.onDownstreamFinish(cause)
              closeIterator()
            }

            override def onPull(): Unit = {
              val future = iterator.onHasNext()

              if (future.isDone && !future.isCompletedExceptionally) {
                if (future.get()) {
                  emit(out, iterator.next())
                }
                else {
                  closeIterator()
                  completeStage()
                }
              }
              else {
                future.asScala onComplete {
                  case Success(hasNext) =>
                    if (hasNext) onIteratorNext.invoke(iterator.next())
                    else onIteratorComplete.invoke(())
                  case Failure(exception) =>
                    onIteratorFailure.invoke(exception)
                }
              }
            }
          }
        )
      }
    }
}
