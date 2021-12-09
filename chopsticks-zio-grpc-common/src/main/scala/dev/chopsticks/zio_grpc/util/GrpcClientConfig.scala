package dev.chopsticks.zio_grpc.util

import eu.timepit.refined.types.net.PortNumber
import eu.timepit.refined.types.string.NonEmptyString
import pureconfig.ConfigConvert

final case class GrpcClientConfig(host: NonEmptyString, port: PortNumber, useTls: Boolean)

object GrpcClientConfig {
  // noinspection TypeAnnotation
  implicit lazy val configConvert = {
    import dev.chopsticks.util.config.PureconfigConverters._
    ConfigConvert[GrpcClientConfig]
  }
}
