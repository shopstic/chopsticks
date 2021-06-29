package dev.chopsticks.kvdb

import zio.Has

package object util {
  type KvdbIoThreadPool = Has[KvdbIoThreadPool.Service]
  type KvdbSerdesThreadPool = Has[KvdbSerdesThreadPool.Service]
}
