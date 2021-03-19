package dev.chopsticks.fp

import zio.Has

package object config {
  type HoconConfig = Has[HoconConfig.Service]
  type TypedConfig[Cfg] = Has[TypedConfig.Service[Cfg]]
}
