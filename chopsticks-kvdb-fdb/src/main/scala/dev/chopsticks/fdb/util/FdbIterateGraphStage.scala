package dev.chopsticks.fdb.util

import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.async.{AsyncIterator, AsyncUtil}
import com.apple.foundationdb.{Database, ReadTransaction, Transaction}

import scala.concurrent.ExecutionContextExecutor
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success}

final class FdbIterateGraphStage[V](db: Database, iterate: ReadTransaction => AsyncIterator[V])
    extends GraphStage[SourceShape[V]] {
  private val out: Outlet[V] = Outlet[V]("FdbIterateGraphStage.out")
  override val shape: SourceShape[V] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      setHandler(out, eagerTerminateOutput)

      private def closeTx(): Unit = {
        pendingTx = pendingTx match {
          case Some(tx) =>
            tx.close()
            None
          case None =>
            None
        }
      }

      override def postStop(): Unit = {
        super.postStop()
        closeTx()
      }

      var pendingTx = Option.empty[Transaction]

      override def preStart(): Unit = {
        implicit val ec: ExecutionContextExecutor = materializer.executionContext
        val onIteratorComplete: AsyncCallback[Unit] = getAsyncCallback[Unit] { (_) => completeStage() }
        val onIteratorNext: AsyncCallback[V] = getAsyncCallback[V] { kv => emit(out, kv) }
        val onIteratorFailure: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { e => failStage(e) }
        val tx = db.createTransaction(ec)
        pendingTx = Some(tx)
        val iterator = iterate(tx.snapshot())

        setHandler(
          out,
          new OutHandler {
            override def onDownstreamFinish(cause: Throwable): Unit = {
              super.onDownstreamFinish(cause)
              closeTx()
            }

            override def onPull(): Unit = {
              val future = iterator.onHasNext()

              if (future eq AsyncUtil.READY_TRUE) {
                emit(out, iterator.next())
              }
              else if (future eq AsyncUtil.READY_FALSE) {
                closeTx()
                completeStage()
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
