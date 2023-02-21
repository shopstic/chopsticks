package dev.chopsticks.kvdb.util

import zio.*
import zio.stream.*

object TestZioProbeOps:
  extension [R, A](stream: ZStream[R, Throwable, A])
    def toTestProbe(implicit trace: Trace): ZIO[R with Scope, Nothing, TestZioProbe[A]] =
      stream
        .rechunk(1)
        .toQueue(capacity = 1)
        .map { queue => TestZioProbe(queue) }
end TestZioProbeOps

object TestZioProbe:
  enum TestZioProbeException(message: String) extends RuntimeException:
    case Timeout(duration: Duration)
        extends TestZioProbeException(s"Probe timed out after waiting for ${duration.toString}.")
    case UnexpectedEndOfStream
        extends TestZioProbeException(s"Unexpected end of stream while waiting for the new element.")
    case UnexpectedElement[A](elem: A) extends TestZioProbeException(s"Received unexpected element: ${elem.toString}.")
    case ElementFailingPf[A](elem: A)
        extends TestZioProbeException(s"Received element failed provided partial function: ${elem.toString}.")
end TestZioProbe

final case class TestZioProbe[A] private[util] (queue: Dequeue[Take[Throwable, A]]):
  import TestZioProbe.*
  import org.scalatest.matchers.should.Matchers._

  def expectMsg(within: Duration): Task[A] =
    queue
      .take
      .flatMap(_.foldZIO(
        ZIO.fail(TestZioProbeException.UnexpectedEndOfStream),
        cause => ZIO.failCause(cause),
        values => ZIO.attempt(values.size should equal(1)).as(values.head)
      ))
      .timeoutFail(TestZioProbeException.Timeout(within))(within)

  def expectStreamCompletion(within: Duration): Task[Unit] =
    expectMsg(within)
      .foldZIO(
        {
          case TestZioProbeException.UnexpectedEndOfStream => ZIO.unit
          case e => ZIO.fail(e)
        },
        elem => ZIO.fail(TestZioProbeException.UnexpectedElement(elem))
      )

  def expectNoMessage(within: Duration)(implicit trace: Trace): Task[Unit] =
    expectMsg(within)
      .foldZIO(
        {
          case _: TestZioProbeException.Timeout => ZIO.unit
          case e => ZIO.fail(e)
        },
        elem => ZIO.fail(TestZioProbeException.UnexpectedElement(elem))
      )

  /** Receive one message and assert that the given partial function accepts it. */
  def expectMsgPF[B](within: Duration)(partialFunction: PartialFunction[A, B])(implicit trace: Trace): Task[B] =
    expectMsg(within)
      .flatMap { elem =>
        if (partialFunction.isDefinedAt(elem)) ZIO.succeed(partialFunction(elem))
        else ZIO.fail(TestZioProbeException.ElementFailingPf(elem))
      }

end TestZioProbe
