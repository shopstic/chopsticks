/*
package dev.chopsticks.stream

import java.io.File

import akka.actor.Status
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Terminated, Timers}
import akka.pattern.ask
import akka.stream.IOResult
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.{ByteString, Timeout}
import cats.Applicative

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

object ChildProcessFlow {
  final case class ProcessExitIOResult(in: IOResult, out: IOResult, err: IOResult, exitValue: Int)

  final case class ChildProcessConfig(
    command: Vector[String],
    workingDir: String = System.getProperty("user.dir"),
    environment: Map[String, String] = Map.empty,
    blockingDispatcherConfigKey: String = "akka.stream.default-blocking-io-dispatcher",
    destroyTimeout: Timeout = Timeout(30.seconds),
    startTimeout: Timeout = Timeout(5.seconds)
  )

  def apply(config: ChildProcessConfig)(implicit
    system: ActorSystem
  ): Flow[ByteString, Out, Future[ProcessExitIOResult]] = {
    Flow
      .lazyFutureFlow(() => {
        val command = config.command
        val workingDir = new File(config.workingDir)
        val environment = config.environment
        val blockingDispatcherConfigKey = config.blockingDispatcherConfigKey
        val destroyTimeout = config.destroyTimeout
        val startTimeout = config.startTimeout

        import system.dispatcher
        val actor = system.actorOf(Props(new ProcessActor(destroyTimeout)))
        implicit val askTimeout: Timeout = startTimeout

        actor
          .ask(StartProcess(command, workingDir, environment, blockingDispatcherConfigKey))
          .mapTo[ProcessStarted]
          .transformWith {
            case util.Success(ProcessStarted(stdin, stdout, stderr)) =>
              val flow = Flow
                .fromSinkAndSourceMat(
                  Flow[ByteString].filter(_.nonEmpty).toMat(stdin)(Keep.right),
                  stdout
                    .map(b => Out(b, Stdout))
                    .mergeMat(stderr.map(b => Out(b, Stderr)))(Keep.both)
                ) { case (in, (out, err)) => (in, out, err) }
                .watchTermination() {
                  case ((in, out, err), done) =>
                    import cats.instances.future._
                    Applicative[Future]
                      .tuple4(in, out, err, done)
                      .transformWith { ioTry =>
                        actor.ask(TerminateProcess).mapTo[ProcessExited].map(_.exitValue).transform {
                          exitValueTry: Try[Int] =>
                            for {
                              (inResult, outResult, errResult, _) <- ioTry
                              exitValue <- exitValueTry
                            } yield ProcessExitIOResult(inResult, outResult, errResult, exitValue)
                        }
                      }
                }

              Future.successful(flow)

            case util.Failure(e) =>
              actor.ask(TerminateProcess).transform(_ => util.Failure(e))
          }
      })
      .mapMaterializedValue(
        _.flatMap(identity)(system.dispatcher)
      )
  }

  sealed trait OutSource

  object Stdout extends OutSource

  object Stderr extends OutSource

  final case class Out(byteString: ByteString, source: OutSource)

  private object ProcessDestroyTimeout

  private object ProcessDestroyForciblyTimeout

  sealed trait Command

  final private case class StartProcess(
    command: Vector[String],
    workingDir: File,
    environment: Map[String, String],
    blockingDispatcherConfigKey: String
  ) extends Command

  private object TerminateProcess extends Command

  sealed trait ProcessState

  final private case class ProcessStarted(
    stdin: Sink[ByteString, Future[IOResult]],
    stdout: Source[ByteString, Future[IOResult]],
    stderr: Source[ByteString, Future[IOResult]]
  ) extends ProcessState

  final private case class ProcessExited(exitValue: Int) extends ProcessState

  private object ProcessDiedUnexpectedly extends RuntimeException with ProcessState

  private object ProcessDestroyForciblyFailure extends RuntimeException with ProcessState

  private class ProcessActor(destroyTimeout: Timeout) extends Actor with ActorLogging with Timers {
    private def pendingStart(state: ActorRef, process: ActorRef): Receive = {
      case ChildProcessActor.ProcessStarted(stdin, stdout, stderr) =>
        state ! Status.Success(ProcessStarted(stdin, stdout, stderr))
        context.become(processing(process))

      case Terminated(`process`) =>
        state ! Status.Failure(ProcessDiedUnexpectedly)
        context.stop(self)
    }

    private def terminating(state: ActorRef, process: ActorRef): Receive = {
      case ProcessDestroyTimeout =>
        process ! ChildProcessActor.DestroyForcibly
        timers.startSingleTimer(ProcessDestroyTimeout, ProcessDestroyForciblyTimeout, destroyTimeout.duration)

      case ProcessDestroyForciblyTimeout =>
        state ! Status.Failure(ProcessDestroyForciblyFailure)
        context.stop(self)

      case ChildProcessActor.ProcessExited(exitValue) =>
        timers.cancel(ProcessDestroyTimeout)
        context.become(processExited(exitValue, process))
        self.tell(TerminateProcess, state)

      case Terminated(`process`) =>
        timers.cancel(ProcessDestroyTimeout)
        context.become(processTerminated)
        self.tell(TerminateProcess, state)
    }

    private def processing(process: ActorRef): Receive = {
      case TerminateProcess =>
        process ! ChildProcessActor.Destroy
        timers.startSingleTimer(ProcessDestroyTimeout, ProcessDestroyTimeout, destroyTimeout.duration)
        context.become(terminating(sender(), process))

      case ChildProcessActor.ProcessExited(exitValue) =>
        context.become(processExited(exitValue, process))

      case Terminated(`process`) =>
        context.become(processTerminated)
    }

    private def processExited(code: Int, process: ActorRef): Receive = {
      case TerminateProcess =>
        sender() ! Status.Success(ProcessExited(code))
        context.stop(self)
      case Terminated(`process`) =>
    }

    private def processTerminated: Receive = {
      case TerminateProcess =>
        sender() ! Status.Failure(ProcessDiedUnexpectedly)
        context.stop(self)
    }

    override def receive: Receive = {
      case StartProcess(command, workingDir, environment, blockingDispatcherConfigKey) =>
        val process =
          context.actorOf(ChildProcessActor.props(command, workingDir, environment, blockingDispatcherConfigKey))
        val _ = context.watch(process)
        context.become(pendingStart(sender(), process))
    }

    override def unhandled(message: Any): Unit = {
      log.error(s"Unhandled message: $message")
      context.stop(self)
    }
  }
}
 */
