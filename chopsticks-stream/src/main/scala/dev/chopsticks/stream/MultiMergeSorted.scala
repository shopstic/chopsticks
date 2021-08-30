package dev.chopsticks.stream

import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

object MultiMergeSorted {
  def merge[T](sources: Seq[Source[T, Any]], untilLastSourceComplete: Boolean = false)(implicit
    ordering: Ordering[T]
  ): Source[T, Any] = {
    Source.fromGraph(GraphDSL.createGraph(new MultiMergeSorted[T](sources.size, ordering, untilLastSourceComplete)) {
      implicit b => merge =>
        import GraphDSL.Implicits._

        sources.foreach { source => source ~> merge }

        SourceShape(merge.out)
    })
  }
}

final class MultiMergeSorted[T] private (
  val inputsCount: Int,
  val ordering: Ordering[T],
  untilLastSourceComplete: Boolean
) extends GraphStage[UniformFanInShape[T, T]] {
  require(inputsCount >= 1, "A Merge must have one or more input ports")

  val ins: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputsCount)(i => Inlet[T]("Merge.in" + i))
  val out: Outlet[T] = Outlet[T]("Merge.out")
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, ins: _*)

  // scalastyle:off method.length
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val buffer = new Array[AnyRef](inputsCount)

    private def itemAt(index: Int): T = buffer(index).asInstanceOf[T]

    private def nextItemToGo(): (Int, T) = {
      var minValue = itemAt(0)
      var minIndex = 0

      for (i <- 1 until buffer.length) {
        val value = itemAt(i)

        // scalastyle:off null
        if (value != null && (minValue == null || ordering.compare(value, minValue) <= 0)) {
          minValue = value
          minIndex = i
        }
        // scalastyle:on null
      }

      (minIndex, minValue)
    }

    private def lastSourceCompleted: Boolean = {
      val index = inputsCount - 1
      // scalastyle:off null
      untilLastSourceComplete && !activeUpstreamsMap(index) && buffer(index) == null
      // scalastyle:on null
    }

    private val activeUpstreamsMap = new Array[Boolean](inputsCount)

    private def bufferReady: Boolean = {
      var ret = true
      var i = 0

      while (ret && i < inputsCount) {
        // scalastyle:off null
        if (activeUpstreamsMap(i) && buffer(i) == null) ret = false
        // scalastyle:on null
        i += 1
      }

      ret
    }

    private def possiblyPushOrComplete(): Unit = {
      if (isAvailable(out) && bufferReady) {
        val (index, item) = nextItemToGo()

        // scalastyle:off null
        if (item != null) {
          buffer.update(index, null)
          push(out, item)
          tryPull(ins(index))

          if (lastSourceCompleted) completeStage()
        }
        // scalastyle:on null
        else completeStage()
      }
      else if (lastSourceCompleted) completeStage()
    }

    private def onItem(index: Int, item: T): Unit = {
      //      println(s"<--| index=$index item=$item")
      buffer.update(index, item.asInstanceOf[AnyRef])
      possiblyPushOrComplete()
    }

    private def onInletComplete(i: Int): Unit = {
      activeUpstreamsMap.update(i, false)
      possiblyPushOrComplete()
    }

    override def preStart(): Unit = {
      ins.foreach(tryPull)
    }

    for (i <- ins.indices) {
      val in = ins(i)
      activeUpstreamsMap.update(i, true)

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val item = grab(in)
            onItem(i, item)
          }

          override def onUpstreamFinish(): Unit = {
            onInletComplete(i)
          }
        }
      )
    }

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          possiblyPushOrComplete()
        }
      }
    )
  }

  override def toString: String = "MultiMergeSorted"
}
