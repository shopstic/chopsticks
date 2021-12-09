package dev.chopsticks.stream

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable
import scala.util.control.NonFatal

final class StatefulMapConcatWithCompleteFlow[In, Out](
  val funs: () => (In => immutable.Iterable[Out], () => immutable.Iterable[Out])
) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("StatefulMapConcatWithCompleteFlow.in")
  val out: Outlet[Out] = Outlet[Out]("StatefulMapConcatWithCompleteFlow.out")
  override val shape = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes.name("statefulMapConcatWithComplete")

  // noinspection TypeAnnotation
  // scalastyle:off
  def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with InHandler with OutHandler {
    lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
    var currentIterator: Iterator[Out] = _
    var (plainFun, completeFun) = funs()
    var isCompleted = false

    def hasNext = if (currentIterator != null) currentIterator.hasNext else false

    setHandlers(in, out, this)

    private def maybeComplete(drain: Boolean) = {
      if (!isCompleted) {
        isCompleted = true
        currentIterator = completeFun().iterator
        if (drain) pushPull()
      }
      else completeStage()
    }

    def pushPull(): Unit =
      if (hasNext) {
        push(out, currentIterator.next())
        if (!hasNext && isClosed(in)) {
          maybeComplete(false)
        }
      }
      else if (!isClosed(in))
        pull(in)
      else {
        maybeComplete(true)
      }

    def onFinish(): Unit = if (!hasNext) {
      maybeComplete(isAvailable(out))
    }

    override def onPush(): Unit =
      try {
        currentIterator = plainFun(grab(in)).iterator
        pushPull()
      }
      catch {
        case NonFatal(ex) =>
          decider(ex) match {
            case Supervision.Stop => failStage(ex)
            case Supervision.Resume => if (!hasBeenPulled(in)) pull(in)
            case Supervision.Restart =>
              restartState()
              if (!hasBeenPulled(in)) pull(in)
          }
      }

    override def onUpstreamFinish(): Unit = onFinish()

    override def onPull(): Unit = {
      pushPull()
    }

    private def restartState(): Unit = {
      val fs = funs()
      plainFun = fs._1
      completeFun = fs._2
      currentIterator = null
    }
  }

  override def toString = "StatefulMapConcatWithCompleteFlow"
}
