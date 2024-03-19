package dev.chopsticks.kvdb.util

import dev.chopsticks.fp.pekko_env.PekkoEnv
import zio.{Executor, URLayer, ZLayer}

import scala.concurrent.ExecutionContextExecutor

trait KvdbSerdesThreadPool {
  def executionContext: ExecutionContextExecutor
  def executor: Executor
}

object KvdbSerdesThreadPool {

  def fromDefaultPekkoDispatcher(): URLayer[PekkoEnv, KvdbSerdesThreadPool] = {
    fromPekkoDispatcher("pekko.actor.default-dispatcher")
  }

  def fromPekkoDispatcher(
    id: String
  ): URLayer[PekkoEnv, KvdbSerdesThreadPool] = {
    val effect = PekkoEnv
      .actorSystem
      .map { actorSystem =>
        new KvdbSerdesThreadPool {
          override val executionContext: ExecutionContextExecutor = actorSystem.dispatchers.lookup(id)
          override val executor: Executor = zio.Executor.fromExecutionContext(executionContext)
        }
      }

    ZLayer(effect)
  }
}
