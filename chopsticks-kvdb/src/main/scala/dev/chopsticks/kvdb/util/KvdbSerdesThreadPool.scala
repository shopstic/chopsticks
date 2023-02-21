package dev.chopsticks.kvdb.util

import zio.{Executor, ULayer, ZLayer}

trait KvdbSerdesThreadPool:
  def executor: Executor

object KvdbSerdesThreadPool:
//  private val DefaultYieldOpCount = 512

  def liveFromDefaultBlockingExecutor: ULayer[KvdbSerdesThreadPool] =
    ZLayer.succeed {
      new KvdbSerdesThreadPool:
        override val executor = zio.Runtime.defaultBlockingExecutor
    }
end KvdbSerdesThreadPool
