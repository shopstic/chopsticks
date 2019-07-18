package dev.chopsticks.kvdb

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.util.control.NoStackTrace

object DbWebsocketServerRequestHandlerFlow {

  val KEEP_ALIVE = TextMessage.Strict("keep-alive")

  final case class InvalidRequestMessageException(msg: String) extends RuntimeException(msg) with NoStackTrace

  object ClientTerminatedPrematurelyException extends RuntimeException with NoStackTrace

}

final class DbWebsocketServerRequestHandlerFlow extends GraphStage[FlowShape[Message, Message]] {

  import DbWebsocketServerRequestHandlerFlow._

  val inlet: Inlet[Message] = Inlet[Message]("DbWebsocketServerRequestHandlerFlow.in")
  val outlet: Outlet[Message] = Outlet[Message]("DbWebsocketServerRequestHandlerFlow.out")

  override val shape = FlowShape(inlet, outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private lazy val keepAliveHandler = new InHandler {
      def onPush(): Unit = {
        val msg = grab(inlet)

        if (msg != KEEP_ALIVE) {
          failStage(
            InvalidRequestMessageException(
              s"Got invalid message after request has already been made (and it's not a keep-alive message): $msg"
            )
          )
        }
        else tryPull(inlet)
      }

      override def onUpstreamFinish(): Unit = {
        failStage(ClientTerminatedPrematurelyException)
      }
    }

    setHandler(
      inlet,
      new InHandler {
        def onPush(): Unit = {
          setHandler(inlet, keepAliveHandler)
          emit(outlet, grab(inlet))
        }

        override def onUpstreamFinish(): Unit = {
          failStage(ClientTerminatedPrematurelyException)
        }
      }
    )

    setHandler(outlet, new OutHandler {
      def onPull(): Unit = {
        tryPull(inlet)
      }
    })
  }
}
