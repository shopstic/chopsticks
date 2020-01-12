package dev.chopsticks.stream

import akka.actor.ActorRef
import akka.stream.stage.{GraphStageLogic, StageLogging}

trait CommonGraphStageLogic extends StageLogging {
  this: GraphStageLogic =>

  type Receive = PartialFunction[Any, Unit]

  implicit lazy val self: ActorRef = stageActor.ref

  protected def logErrorAndFailStage(ex: Throwable): Unit = {
    log.error(ex, ex.toString)
    failStage(ex)
  }

  protected val unhandled: Receive = {
    case m => logErrorAndFailStage(new Exception(s"Unexpected message: ${m.toString}"))
  }

  protected val doNothing: () => Unit = () => {}

  protected def makeHandler(handle: Receive): ((ActorRef, Any)) => Unit = {
    val combined = handle orElse unhandled
    (event: (ActorRef, Any)) => {
      try {
        combined(event._2)
      }
      catch {
        case ex: Throwable =>
          log.error(s"Bad handler with uncaught exception: {}", ex)
          failStage(ex)
      }
    }
  }
}
