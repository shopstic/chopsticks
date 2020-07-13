package dev.chopsticks.fp

import zio.Has

package object util {
  type SharedResourceManager[R, Id, Res] = Has[SharedResourceManager.Service[R, Id, Res]]
  type SharedResourceFactory[R, Id, Res] = Has[SharedResourceFactory.Service[R, Id, Res]]
}
