package dev.chopsticks.zio_grpc.util

import io.grpc.netty.NettyChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import eu.timepit.refined.auto._

object ZioGrpcUtils {
  def manageChannel(config: GrpcClientConfig): ZManagedChannel[Any] = {
    val channelBuilder = NettyChannelBuilder
      .forAddress(config.host, config.port)
    ZManagedChannel(if (config.useTls) channelBuilder else channelBuilder.usePlaintext())
  }
}
