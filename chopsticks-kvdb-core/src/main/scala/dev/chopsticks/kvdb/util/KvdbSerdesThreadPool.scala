package dev.chopsticks.kvdb.util

import dev.chopsticks.fp.akka_env.AkkaEnv
import zio.URLayer
import zio.internal.Executor

import scala.concurrent.ExecutionContextExecutor

object KvdbSerdesThreadPool {
  private val DefaultYieldOpCount = 512

  trait Service {
    def executionContext: ExecutionContextExecutor
    def executor: Executor
  }

  def fromDefaultAkkaDispatcher(yieldOpCount: Int = DefaultYieldOpCount): URLayer[AkkaEnv, KvdbSerdesThreadPool] = {
    fromAkkaDispatcher("akka.actor.default-dispatcher", yieldOpCount)
  }

  def fromAkkaDispatcher(
    id: String,
    yieldOpCount: Int = DefaultYieldOpCount
  ): URLayer[AkkaEnv, KvdbSerdesThreadPool] = {
    AkkaEnv
      .actorSystem
      .map { actorSystem =>
        new Service {
          override val executionContext: ExecutionContextExecutor = actorSystem.dispatchers.lookup(id)
          override val executor: Executor = zio.internal.Executor.fromExecutionContext(yieldOpCount)(executionContext)
        }
      }
      .toLayer
  }
}
