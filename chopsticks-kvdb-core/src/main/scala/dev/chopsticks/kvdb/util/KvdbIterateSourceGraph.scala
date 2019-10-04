package dev.chopsticks.kvdb.util

import akka.stream.KillSwitches.KillableGraphStageLogic
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import dev.chopsticks.kvdb.util.KvdbAliases.{KvdbBatch, KvdbPair}

import scala.collection.mutable

object KvdbIterateSourceGraph {
  final case class Refs(iterator: Iterator[KvdbPair], close: () => Unit)
}

class KvdbIterateSourceGraph(
  createRefs: () => Either[Exception, KvdbIterateSourceGraph.Refs],
  closeSignal: KvdbCloseSignal,
  dispatcher: String
)(implicit clientOptions: KvdbClientOptions)
    extends GraphStage[SourceShape[KvdbBatch]] {
  import KvdbIterateSourceGraph._
  private val maxBatchBytes = clientOptions.maxBatchBytes.toBytes.toInt

  val outlet: Outlet[KvdbBatch] = Outlet("KvdbIterateSourceGraph.out")

  override protected def initialAttributes = ActorAttributes.dispatcher(dispatcher)

  val shape: SourceShape[KvdbBatch] = SourceShape[KvdbBatch](outlet)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    val shutdownListener = closeSignal.createListener()

    new KillableGraphStageLogic(shutdownListener.future, shape) with StageLogging {
      var refs: Refs = _

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

              while (batchSizeSoFar < maxBatchBytes && iterator.hasNext) {
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
