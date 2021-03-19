package dev.chopsticks.fp

import zio.Has

package object akka_env {
  type AkkaEnv = Has[AkkaEnv.Service]
  type AkkaRuntime[R] = Has[AkkaRuntime.Service[R]]
}
