package dev.chopsticks.kvdb.util

import zio.{Executor, ULayer, ZLayer}

trait KvdbIoThreadPool:
//  def executionContext: ExecutionContextExecutor
  def executor: Executor

object KvdbIoThreadPool {

  def live: ULayer[KvdbIoThreadPool] = {
    ZLayer.succeed {
      new KvdbIoThreadPool:
//        override def executionContext = zio.Runtime.defaultExecutor
        override def executor = zio.Runtime.defaultExecutor
    }
  }
}
