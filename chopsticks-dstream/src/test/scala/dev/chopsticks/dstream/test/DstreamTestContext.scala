package dev.chopsticks.dstream.test

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.stream.scaladsl.Source
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import dev.chopsticks.dstream.DstreamState.WorkResult
import zio.{Fiber, Has, URIO, ZIO}

object DstreamTestContext {
  def get[A: zio.Tag, R: zio.Tag]: URIO[Has[DstreamTestContext[A, R]], DstreamTestContext[A, R]] =
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
