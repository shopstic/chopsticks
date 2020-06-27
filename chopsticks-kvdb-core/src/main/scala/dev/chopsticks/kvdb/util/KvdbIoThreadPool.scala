package dev.chopsticks.kvdb.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{SynchronousQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import dev.chopsticks.fp.akka_env.AkkaEnv
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import zio.internal.Executor
import zio.{ULayer, URLayer, ZLayer, ZManaged}

object KvdbIoThreadPool {
  trait Service {
    def executor: Executor
  }

  def fromAkkaDispatcher(id: String): URLayer[AkkaEnv, KvdbIoThreadPool] = {
    val managed = for {
      akkaService <- ZManaged.access[AkkaEnv](_.get)
    } yield {
      new Service {
        override val executor: Executor =
          zio.internal.Executor.fromExecutionContext(Int.MaxValue)(akkaService.actorSystem.dispatchers.lookup(id))
      }
    }

    managed.toLayer
  }

  def live(
    name: NonEmptyString = "dev.chopsticks.kvdb.io",
    corePoolSize: Int = 0,
    maxPoolSize: Int = 128,
    keepAliveTimeMs: Long = 60000
  ): ULayer[KvdbIoThreadPool] = {
    ZLayer.succeed(
      new Service {
        override val executor: Executor = zio.internal.Executor.fromThreadPoolExecutor(_ => Int.MaxValue) {
          val timeUnit = TimeUnit.MILLISECONDS
          val workQueue = new SynchronousQueue[Runnable]()
          val threadFactory = new KvdbIoThreadFactory(name, true)

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
      }
    )
  }

  final class KvdbIoThreadFactory(name: String, daemon: Boolean) extends ThreadFactory {
    private val parentGroup =
      Option(System.getSecurityManager).fold(Thread.currentThread().getThreadGroup)(_.getThreadGroup)

    private val threadGroup = new ThreadGroup(parentGroup, name)
    private val threadCount = new AtomicInteger(1)
    private val threadHash = Integer.toUnsignedString(this.hashCode())

    override def newThread(r: Runnable): Thread = {
      val newThreadNumber = threadCount.getAndIncrement()

      val thread = new Thread(threadGroup, r)
      thread.setName(s"$name-$newThreadNumber-$threadHash")
      thread.setDaemon(daemon)

      thread
    }
  }
}
