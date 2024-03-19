package dev.chopsticks.dstream.test

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.testkit.{TestPublisher, TestSubscriber}
import dev.chopsticks.dstream.DstreamState.WorkResult
import zio.{Fiber, URIO, ZIO}

object DstreamTestContext {
  def get[A: zio.Tag, R: zio.Tag]: URIO[DstreamTestContext[A, R], DstreamTestContext[A, R]] =
    ZIO.service[DstreamTestContext[A, R]]
}

final case class DstreamTestContext[A, R](
  serverBinding: Http.ServerBinding,
  masterAssignments: TestPublisher.Probe[A],
  masterRequests: zio.Queue[(A, WorkResult[R])],
  masterResponses: zio.Queue[A],
  masterOutputs: TestSubscriber.Probe[A],
  workerRequests: zio.Queue[A],
  workerResponses: zio.Queue[Source[R, NotUsed]],
  masterFiber: Fiber.Runtime[Throwable, Unit],
  workerFiber: Fiber.Runtime[Throwable, Unit]
)
