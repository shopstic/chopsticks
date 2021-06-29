package dev.chopsticks.kvdb.util

import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import zio.blocking.Blocking
import zio.internal.Executor
import zio.{UIO, URLayer, ZManaged}

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.ExecutionContextExecutor

object KvdbSerdesThreadPool {
  private val DefaultYieldOpCount = 512

  trait Service {
    def executionContext: ExecutionContextExecutor
    def executor: Executor
  }

  def fromAkkaDispatcher(
    id: String,
    yieldOpCount: Int = DefaultYieldOpCount
  ): URLayer[AkkaEnv, KvdbSerdesThreadPool] = {
    ZManaged
      .access[AkkaEnv](_.get)
      .map { akkaService =>
        new Service {
          override val executionContext: ExecutionContextExecutor = akkaService.actorSystem.dispatchers.lookup(id)
          override val executor: Executor = zio.internal.Executor.fromExecutionContext(yieldOpCount)(executionContext)
        }
      }
      .toLayer
  }

  def live(
    name: NonEmptyString = "dev.chopsticks.kvdb.serdes",
    corePoolSize: Int = 8,
    maxPoolSize: Int = 128,
    keepAliveTimeMs: Long = 10000,
    yieldOpCount: Int = DefaultYieldOpCount
  ): URLayer[Blocking, KvdbSerdesThreadPool] = {
    ZManaged.make {
      UIO {
        val timeUnit = TimeUnit.MILLISECONDS
        val workQueue = new LinkedBlockingQueue[Runnable]()
        val threadFactory = new KvdbThreadFactory(name, true)

        val threadPool = new ThreadPoolExecutor(
          corePoolSize,
          maxPoolSize,
          keepAliveTimeMs,
          timeUnit,
          workQueue,
          threadFactory
        )

        threadPool
      }
    } { threadPool =>
      zio.blocking.effectBlocking {
        threadPool.shutdown()
        threadPool.awaitTermination(5, TimeUnit.SECONDS)
      }.orDie
    }
      .map { pool =>
        new Service {
          override val executor: Executor = zio.internal.Executor.fromThreadPoolExecutor(_ => yieldOpCount)(pool)
          override val executionContext: ExecutionContextExecutor = {
            val ec = executor.asEC
            new ExecutionContextExecutor {
              override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)
              override def execute(command: Runnable): Unit = ec.execute(command)
            }
          }
        }
      }
      .toLayer
  }
}
