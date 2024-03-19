package dev.chopsticks.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

object StatefulConflateFlow {
  def apply[In, Out, State](
    seed: State,
    accumulator: (State, In) => State,
    emitter: State => (State, Option[Out])
  ): Flow[In, Out, NotUsed] = Flow.fromGraph(new StatefulConflateFlow(seed, accumulator, emitter))
}

final class StatefulConflateFlow[In, Out, State](
  seed: State,
  accumulator: (State, In) => State,
  emitter: State => (State, Option[Out])
) extends GraphStage[FlowShape[In, Out]] {
  val in: Inlet[In] = Inlet[In]("StatefulConflateFlow.in")
  val out: Outlet[Out] = Outlet[Out]("StatefulConflateFlow.out")
  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes.name("statefulConflateFlow")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      setHandlers(in, out, this)

      private var state = seed
      private var isStateEmpty = false
      private var upstreamFinished = false

      private def maybeCompleteStage(): Unit = {
        if (isStateEmpty && upstreamFinished) {
          completeStage()
        }
      }

      private def maybeEmitNext(): Unit = {
        val (newState, maybeToEmit) = emitter(state)
        state = newState

        isStateEmpty = maybeToEmit match {
          case Some(toEmit) =>
            emit(out, toEmit)
            false
          case None =>
            true
        }

        maybeCompleteStage()
      }

      override def preStart(): Unit = {
        pull(in)
      }

      override def onPush(): Unit = {
        state = accumulator(state, grab(in))
        pull(in)
        if (isAvailable(out)) {
          maybeEmitNext()
        }
      }

      override def onPull(): Unit = {
        maybeEmitNext()
      }

      override def onUpstreamFinish(): Unit = {
        upstreamFinished = true
        maybeCompleteStage()
      }
    }
}
