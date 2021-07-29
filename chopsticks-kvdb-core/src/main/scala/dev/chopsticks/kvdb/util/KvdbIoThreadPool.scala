package dev.chopsticks.kvdb.util

import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.URLayer
import zio.internal.Executor

import scala.concurrent.ExecutionContextExecutor

object KvdbIoThreadPool {
  trait Service {
    def executionContext: ExecutionContextExecutor
    def executor: Executor
  }

  def live: URLayer[AkkaEnv, KvdbIoThreadPool] = {
    fromAkkaDispatcher("dev.chopsticks.kvdb.io-dispatcher")
  }

  def fromAkkaDispatcher(id: String): URLayer[AkkaEnv, KvdbIoThreadPool] = {
    AkkaEnv
      .actorSystem
      .map { actorSystem =>
        new Service {
          override val executionContext: ExecutionContextExecutor = actorSystem.dispatchers.lookup(id)
          override val executor: Executor =
            zio.internal.Executor.fromExecutionContext(Int.MaxValue)(executionContext)
        }
      }
      .toLayer
  }
}
