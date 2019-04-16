package com.shopstic.stream

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object LastStateFlow {

  def apply[E, S, R](seed: => S, next: (S, E) => S, result: S => R): LastStateFlow[E, S, R] =
    new LastStateFlow[E, S, R](seed, next, result)
}

final class LastStateFlow[E, S, R] private (seed: => S, next: (S, E) => S, result: S => R)
    extends GraphStageWithMaterializedValue[FlowShape[E, E], Future[(R, Try[Done])]] {

  override val shape = FlowShape(Inlet[E]("LastStateFlow.in"), Outlet[E]("LastStateFlow.out"))

  override def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[(R, Try[Done])]) = {

    val matValue = Promise[(R, Try[Done])]()

    val logic: GraphStageLogic = new GraphStageLogic(shape) {
      import shape._

      //noinspection ScalaStyle
      // scalastyle:off null
      private var currentState: S = seed

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(in)
            currentState = next(currentState, element)
            push(out, element)
          }

          override def onUpstreamFinish(): Unit = {
            val _ = matValue.success((result(currentState), Success(Done)))
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(t: Throwable): Unit = {
            val _ = matValue.success((result(currentState), Failure(t)))
            super.onUpstreamFinish()
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = pull(in)

          override def onDownstreamFinish(): Unit = {
            val _ = matValue.success((result(currentState), Success(Done)))
            super.onDownstreamFinish()
          }
        }
      )
    }

    (logic, matValue.future)
  }
}
