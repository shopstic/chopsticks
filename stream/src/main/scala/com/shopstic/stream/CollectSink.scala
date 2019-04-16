package com.shopstic.stream
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{AbruptStageTerminationException, Attributes, Inlet, SinkShape}

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Future, Promise}

//noinspection ScalaStyle
final class CollectSink[T, That](implicit cbf: CanBuildFrom[That, T, That])
    extends GraphStageWithMaterializedValue[SinkShape[T], Future[That]] {
  val in: Inlet[T] = Inlet[T]("collection.in")

  override def toString: String = "CollectSink"

  override val shape: SinkShape[T] = SinkShape.of(in)

  //noinspection TypeAnnotation
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val p: Promise[That] = Promise()
    val logic = new GraphStageLogic(shape) with InHandler {
      val buf = cbf()

      override def preStart(): Unit = pull(in)

      def onPush(): Unit = {
        val _ = buf += grab(in)
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        val result = buf.result()
        val _ = p.trySuccess(result)
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        val _ = p.tryFailure(ex)
        failStage(ex)
      }

      override def postStop(): Unit = {
        if (!p.isCompleted) {
          val _ = p.failure(new AbruptStageTerminationException(this))
        }
      }

      setHandler(in, this)
    }

    (logic, p.future)
  }
}
