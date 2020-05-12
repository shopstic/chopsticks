package dev.chopsticks.fp

import zio.Has

package object iz_logging {
  type IzLogging = Has[IzLogging.Service]
}
