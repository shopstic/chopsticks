package dev.chopsticks

import zio.Has

package object jwt {
  type JwtEncoder[C] = Has[JwtEncoder.Service[C]]
  type JwtDecoder[C] = Has[JwtDecoder.Service[C]]
}
