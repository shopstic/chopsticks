package dev.chopsticks

import dev.chopsticks.zio_grpc.ZioGrpcServer.Binding
import zio.Has

package object zio_grpc {
  type ZioGrpcServer[B <: Binding] = Has[ZioGrpcServer.Service[B]]
}
