package dev.chopsticks.kvdb.util

import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio.{Executor, URLayer, ZLayer}

import scala.concurrent.ExecutionContextExecutor

trait KvdbIoThreadPool {
  def executionContext: ExecutionContextExecutor
  def executor: Executor
}

object KvdbIoThreadPool {

  def live: URLayer[PekkoEnv, KvdbIoThreadPool] = {
    fromPekkoDispatcher("dev.chopsticks.kvdb.io-dispatcher")
  }

  def fromPekkoDispatcher(id: String): URLayer[PekkoEnv, KvdbIoThreadPool] = {
    val effect = PekkoEnv
      .actorSystem
      .map { actorSystem =>
        new KvdbIoThreadPool {
          override val executionContext: ExecutionContextExecutor = actorSystem.dispatchers.lookup(id)
          override val executor: Executor =
            zio.Executor.fromExecutionContext(executionContext)
        }
      }

    ZLayer(effect)
  }
}
