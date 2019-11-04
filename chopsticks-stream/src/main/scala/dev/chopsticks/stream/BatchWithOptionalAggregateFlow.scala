package dev.chopsticks.stream

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, Supervision}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.util.control.NonFatal

final case class BatchWithOptionalAggregateFlow[In, Out](
  max: Long,
  costFn: In => Long,
  seed: In => Out,
  aggregate: (Out, In) => Option[Out]
) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("Batch.in")
  val out: Outlet[Out] = Outlet[Out]("Batch.out")

  override val shape: FlowShape[In, Out] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      lazy val decider: Decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var agg: Out = null.asInstanceOf[Out]
      private var left: Long = max
      private var pending: In = null.asInstanceOf[In]

      private def flush(): Unit = {
        if (agg != null) {
          push(out, agg)
          left = max
        }
        if (pending != null) {
          try {
            agg = seed(pending)
            left -= costFn(pending)
            pending = null.asInstanceOf[In]
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart => restartState()
                case Supervision.Resume =>
                  pending = null.asInstanceOf[In]
              }
          }
        }
        else {
          agg = null.asInstanceOf[Out]
        }
      }

      override def preStart() = pull(in)

      def onPush(): Unit = {
        val elem = grab(in)
        val cost = costFn(elem)

        if (agg == null) {
          try {
            agg = seed(elem)
            left -= cost
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart =>
                  restartState()
                case Supervision.Resume =>
              }
          }
        }
        else if (left < cost) {
          pending = elem
        }
        else {
          try {
            aggregate(agg, elem) match {
              case Some(newAgg) =>
                agg = newAgg
                left -= cost
              case None =>
                pending = elem
            }
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop => failStage(ex)
                case Supervision.Restart =>
                  restartState()
                case Supervision.Resume =>
              }
          }
        }

        if (isAvailable(out)) flush()
        if (pending == null) pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (agg == null) completeStage()
      }

      def onPull(): Unit = {
        if (agg == null) {
          if (isClosed(in)) completeStage()
          else if (!hasBeenPulled(in)) pull(in)
        }
        else if (isClosed(in)) {
          push(out, agg)
          if (pending == null) completeStage()
          else {
            try {
              agg = seed(pending)
            } catch {
              case NonFatal(ex) =>
                decider(ex) match {
                  case Supervision.Stop => failStage(ex)
                  case Supervision.Resume =>
                  case Supervision.Restart =>
                    restartState()
                    if (!hasBeenPulled(in)) pull(in)
                }
            }
            pending = null.asInstanceOf[In]
          }
        }
        else {
          flush()
          if (!hasBeenPulled(in)) pull(in)
        }
      }

      private def restartState(): Unit = {
        agg = null.asInstanceOf[Out]
        left = max
        pending = null.asInstanceOf[In]
      }

      setHandlers(in, out, this)
    }
}
