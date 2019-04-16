package com.shopstic.stream
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.util.control.NonFatal

final class StatefulMapWithCompleteFlow[In, Out](val funs: () => (In => Out, () => Option[Out]))
    extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("StatefulMapWithCompleteFlow.in")
  val out: Outlet[Out] = Outlet[Out]("StatefulMapWithCompleteFlow.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes.name("statefulMapWithCompleteFlow")

  //noinspection TypeAnnotation
  // scalastyle:off
  def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
    lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    var (plainFun, completeFun) = funs()
    var isCompleted = false
    var lastEmit = Option.empty[Out]

    setHandlers(in, out, this)

    private def maybeComplete() = {
      if (!isCompleted) {
        isCompleted = true
        lastEmit = completeFun()

        if (lastEmit.isEmpty) completeStage()
        else if (isAvailable(out)) {
          push(out, lastEmit.get)
          completeStage()
        }
      }
    }

    override def onPush(): Unit = {
      try {
        val item = plainFun(grab(in))
        push(out, item)
      } catch {
        case NonFatal(ex) =>
          decider(ex) match {
            case Supervision.Stop => failStage(ex)
            case Supervision.Resume => if (!hasBeenPulled(in)) pull(in)
            case Supervision.Restart =>
              restartState()
              if (!hasBeenPulled(in)) pull(in)
          }
      }
    }

    override def onUpstreamFinish(): Unit = maybeComplete()

    override def onPull(): Unit = {
      if (isCompleted) {
        lastEmit.foreach(push(out, _))
        completeStage()
      }
      else {
        tryPull(in)
      }
    }

    private def restartState(): Unit = {
      val fs = funs()
      plainFun = fs._1
      completeFun = fs._2
    }
  }

  override def toString = "StatefulMapWithCompleteFlow"

}
