package dev.chopsticks

import zio._

package object dstream {
  type DstreamStateFactory = Has[DstreamStateFactory.Service]
}
