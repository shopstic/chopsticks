package dev.chopsticks.kvdb.util

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import zio.blocking.Blocking
import zio.internal.Executor
import zio.{UIO, URLayer, ZManaged}

object KvdbIoThreadPool {
  private val DefaultYieldOpCount = 2048

  trait Service {
    def executor: Executor
  }

  def fromAkkaDispatcher(id: String, yieldOpCount: Int = DefaultYieldOpCount): URLayer[AkkaEnv, KvdbIoThreadPool] = {
    val managed = for {
      akkaService <- ZManaged.access[AkkaEnv](_.get)
    } yield {
      new Service {
        override val executor: Executor =
          zio.internal.Executor.fromExecutionContext(yieldOpCount)(akkaService.actorSystem.dispatchers.lookup(id))
      }
    }

    managed.toLayer
  }

  def live(
    name: NonEmptyString = "dev.chopsticks.kvdb.io",
    corePoolSize: Int = 0,
    maxPoolSize: Int = 128,
    keepAliveTimeMs: Long = 60000,
    yieldOpCount: Int = DefaultYieldOpCount
  ): URLayer[Blocking, KvdbIoThreadPool] = {
    ZManaged.make {
      UIO {
        val timeUnit = TimeUnit.MILLISECONDS
        val workQueue = new SynchronousQueue[Runnable]()
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
        }
      }
      .toLayer
  }

}
