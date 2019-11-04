package dev.chopsticks.stream

import java.io.File
import java.lang.{Process => JavaProcess, ProcessBuilder => JavaProcessBuilder}

import akka.actor.{Actor, ActorLogging, NoSerializationVerificationNeeded, Props, Status, SupervisorStrategy}
import akka.dispatch.MessageDispatcher
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.stream.{ActorAttributes, IOResult}
import akka.util.{ByteString, Helpers}

import scala.collection.{immutable, JavaConverters}
import scala.concurrent.Future
import scala.util.control.NoStackTrace

object ChildProcessActor {
  final case class ProcessStarted(
    stdin: Sink[ByteString, Future[IOResult]],
    stdout: Source[ByteString, Future[IOResult]],
    stderr: Source[ByteString, Future[IOResult]]
  ) extends NoSerializationVerificationNeeded

  final case class ProcessFailure(message: String, cause: Throwable) extends RuntimeException with NoStackTrace

  final case class ProcessExited(exitValue: Int)

  case object Destroy

  case object DestroyForcibly

  private case object StdinTerminated

  private case object StdoutTerminated

  private case object StderrTerminated

  final private case class IOStreamsCompletionState(
    stdin: Boolean = false,
    stdout: Boolean = false,
    stderr: Boolean = false
  )

  def props(
    command: immutable.Seq[String],
    workingDir: File,
    environment: Map[String, String],
    blockingDispatcherConfigKey: String
  ): Props = {
    Props(new ChildProcessActor(command, workingDir, environment, blockingDispatcherConfigKey))
  }

  private def needsQuoting(s: String) = s.isEmpty || (s exists (c => c == ' ' || c == '\t' || c == '\\' || c == '"'))

  private def winQuote(s: String): String = {
    if (needsQuoting(s)) {
      val quoted = s.replaceAll("""([\\]*)"""", """$1$1\\"""").replaceAll("""([\\]*)\z""", "$1$1")
      s""""$quoted""""
    }
    else {
      s
    }
  }
}

class ChildProcessActor(
  command: immutable.Seq[String],
  directory: File,
  environment: Map[String, String],
  blockingDispatcherConfigKey: String
) extends Actor
    with ActorLogging {
  import ChildProcessActor._

  implicit val blockingEc: MessageDispatcher = context.system.dispatchers.lookup(blockingDispatcherConfigKey)

  override val supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  private val process: JavaProcess = {
    import JavaConverters._
    val preparedCommand = if (Helpers.isWindows) command.map(winQuote) else command
    val pb = new JavaProcessBuilder(preparedCommand.asJava)
    pb.environment().putAll(environment.asJava)
    val _ = pb.directory(directory)
    pb.start()
  }

  private def createIOStreams: Future[ProcessStarted] = {
    Future {
      val selfDispatcherAttribute = ActorAttributes.dispatcher(blockingDispatcherConfigKey)

      val stdin: Sink[ByteString, Future[IOResult]] = StreamConverters
        .fromOutputStream(() => process.getOutputStream)
        .withAttributes(selfDispatcherAttribute)
        .mapMaterializedValue(_.andThen { case _ => self ! StdinTerminated })

      val stdout: Source[ByteString, Future[IOResult]] = StreamConverters
        .fromInputStream(() => process.getInputStream)
        .withAttributes(selfDispatcherAttribute)
        .mapMaterializedValue(_.andThen { case _ => self ! StdoutTerminated })

      val stderr: Source[ByteString, Future[IOResult]] = StreamConverters
        .fromInputStream(() => process.getErrorStream)
        .withAttributes(selfDispatcherAttribute)
        .mapMaterializedValue(_.andThen { case _ => self ! StderrTerminated })

      log.debug(s"Process started")
      ProcessStarted(stdin, stdout, stderr)
    }
  }

  override def preStart(): Unit = {
    val _ = createIOStreams.pipeTo(self)
  }

  private def maybeComplete(state: IOStreamsCompletionState): Unit = {
    state match {
      case IOStreamsCompletionState(true, true, true) =>
        log.debug("All IOs are completed, waiting for process exit")
        val _ = Future(process.waitFor()).map(ProcessExited.apply).pipeTo(self)
        context.become(waitingForExit)
      case _ =>
        context.become(processing(state))
    }
  }

  private def waitingForExit: Receive = {
    case Destroy =>
    case DestroyForcibly =>
      log.debug("Received request to forcibly destroy the process.")
      val _ = process.destroyForcibly()
      context.parent ! ProcessExited(9999)
      context.stop(self)

    case m: ProcessExited =>
      context.parent ! m
      context.stop(self)

    case Status.Failure(e) =>
      context.parent ! ProcessFailure("Failed waiting for process exit", e)
      context.stop(self)
  }

  private def processing(ioStreamsState: IOStreamsCompletionState): Receive = {
    case Destroy =>
      val _ = Future {
        process.destroy()
        process.waitFor()
      }.map(ProcessExited.apply).pipeTo(self)
      context.become(waitingForExit)

    case DestroyForcibly =>
      log.debug("Received request to forcibly destroy the process.")
      val _ = process.destroyForcibly()
      context.parent ! ProcessExited(9999)
      context.stop(self)

    case StdinTerminated =>
      log.debug("Stdin was terminated")
      maybeComplete(ioStreamsState.copy(stdin = true))

    case StdoutTerminated =>
      log.debug("Stdout was terminated")
      maybeComplete(ioStreamsState.copy(stdout = true))

    case StderrTerminated =>
      log.debug("Stderr was terminated")
      maybeComplete(ioStreamsState.copy(stderr = true))
  }

  override def unhandled(message: Any): Unit = {
    log.error(s"Unhandled message: $message")
    context.stop(self)
  }

  override def receive: Receive = {
    case m: ProcessStarted =>
      context.parent ! m
      context.become(processing(IOStreamsCompletionState()))

    case Status.Failure(e) =>
      context.parent ! ProcessFailure("Failed starting process", e)
      context.stop(self)
  }

  override def postStop(): Unit = {
    if (process.isAlive) {
      log.warning("Process is still alive in postStop(), trying to destroy forcibly...")
      val _ = process.destroyForcibly()
    }
  }
}
