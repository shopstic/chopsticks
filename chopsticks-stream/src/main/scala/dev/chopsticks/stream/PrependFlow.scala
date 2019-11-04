package dev.chopsticks.stream

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

object PrependFlow {
  def apply[V](initialValue: => V): PrependFlow[V] = new PrependFlow[V](initialValue)
}

final class PrependFlow[V] private (initialValue: => V) extends GraphStage[FlowShape[V, V]] {
  val shape = FlowShape(Inlet[V]("MemoryFlow.in"), Outlet[V]("MemoryFlow.out"))

  def createLogic(attributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    import shape._

    setHandler(
      in,
      new InHandler {
        def onPush(): Unit = {
          val element = grab(in)
          push(out, element)
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        def onPull(): Unit = {
          push(out, initialValue)
          setHandler(
            out,
            new OutHandler {
              def onPull(): Unit = pull(in)
            }
          )
        }
      }
    )
  }
}
