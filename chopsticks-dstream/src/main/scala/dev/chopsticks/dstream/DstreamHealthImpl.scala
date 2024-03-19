package dev.chopsticks.dstream

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import grpc.health.v1.HealthCheckResponse.ServingStatus
import grpc.health.v1.{Health, HealthCheckRequest, HealthCheckResponse}

import scala.concurrent.Future
import scala.concurrent.duration._

final class DstreamHealthImpl extends Health {
  override def check(in: HealthCheckRequest): Future[HealthCheckResponse] = {
    Future.successful(HealthCheckResponse(ServingStatus.SERVING))
  }

  override def watch(in: HealthCheckRequest): Source[HealthCheckResponse, NotUsed] = {
    Source
      .tick(Duration.Zero, 1.second, HealthCheckResponse(ServingStatus.SERVING))
      .mapMaterializedValue(_ => NotUsed)
  }
}
