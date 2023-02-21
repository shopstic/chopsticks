/*
package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._

import scala.util.control.{NoStackTrace, NonFatal}

object FailIfEmptyFlow {
  object UpstreamFinishWithoutEmittingAnyItemException extends RuntimeException with NoStackTrace

  def apply[V]: Flow[V, V, NotUsed] = Flow.fromGraph(new FailIfEmptyFlow[V]())
}

final class FailIfEmptyFlow[V] extends GraphStage[FlowShape[V, V]] {
  import FailIfEmptyFlow._

  val in = Inlet[V]("FailIfEmptyFlow.in")
  val out = Outlet[V]("FailIfEmptyFlow.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private def decider =
        inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var isEmpty = true

      override def onUpstreamFinish(): Unit = {
        if (isEmpty) {
          failStage(UpstreamFinishWithoutEmittingAnyItemException)
        }
        else {
          super.onUpstreamFinish()
        }
      }

      override def onPush(): Unit = {
        try {
          push(out, grab(in))
          isEmpty = false
        }
        catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop => failStage(ex)
              case _ => pull(in)
            }
        }
      }

      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}
 */
