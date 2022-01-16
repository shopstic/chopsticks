package dev.chopsticks.fp.iz_logging

import akka.actor.Status
import akka.stream.scaladsl.{Keep, RestartFlow, Source, Tcp}
import akka.stream.{CompletionStrategy, KillSwitches, OverflowStrategy, RestartSettings}
import akka.util.ByteString
import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import izumi.logstage.api.Log
import izumi.logstage.api.logger.LogSink
import izumi.logstage.api.rendering.RenderingPolicy

import scala.util.Failure

final class IzLoggingTcpSink(
  host: NonEmptyString,
  port: PortNumber,
  renderingPolicy: RenderingPolicy,
  bufferSize: PosInt,
  tcpFlowRestartSettings: RestartSettings,
  akkaSvc: AkkaEnv.Service
) extends LogSink {
  private val ((sourceActorRef, killSwitch), future) = {
    import akkaSvc.actorSystem

    val source = Source.actorRef[String](
      {
        case Status.Success(s: CompletionStrategy) => s
        case Status.Success(_) => CompletionStrategy.draining
        case Status.Success => CompletionStrategy.draining
      }: PartialFunction[Any, CompletionStrategy],
      { case Status.Failure(cause) => cause }: PartialFunction[Any, Throwable],
      bufferSize.value,
      OverflowStrategy.dropHead
    )

    val flow = RestartFlow.withBackoff(tcpFlowRestartSettings) { () =>
      Tcp().outgoingConnection(host.value, port.value)
    }

    source
      .map(ByteString.fromString)
      .viaMat(KillSwitches.single)(Keep.both)
      .viaMat(flow)(Keep.left)
      .toMat(akka.stream.scaladsl.Sink.ignore)(Keep.both)
      .run()
  }

  locally {
    import akkaSvc.dispatcher
    future.onComplete {
      case Failure(exception) =>
        Console.err.println("IzLoggingTcpSink failed")
        exception.printStackTrace(Console.err)
      case _ =>
    }
  }

  override def flush(e: Log.Entry): Unit = {
    sourceActorRef ! renderingPolicy.render(e) + "\n"
  }

  override def close(): Unit = {
    killSwitch.shutdown()
  }
}
