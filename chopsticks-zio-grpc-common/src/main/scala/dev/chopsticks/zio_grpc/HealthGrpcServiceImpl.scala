package dev.chopsticks.zio_grpc

import grpc.health.v1.HealthCheckResponse.ServingStatus
import grpc.health.v1.ZioV1Health.RHealth
import grpc.health.v1.{HealthCheckRequest, HealthCheckResponse}
import io.grpc.Status
import zio.ZIO
import zio.clock.Clock
import zio.stream.ZStream

import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

final class HealthGrpcServiceImpl extends RHealth[Clock] {
  override def check(request: HealthCheckRequest): ZIO[Clock, Status, HealthCheckResponse] = {
    ZIO.succeed(HealthCheckResponse(status = ServingStatus.SERVING))
  }

  override def watch(request: HealthCheckRequest): ZStream[Clock, Status, HealthCheckResponse] = {
    ZStream
      .tick(1.second.toJava)
      .as(HealthCheckResponse(status = ServingStatus.SERVING))
  }
}
