package dev.chopsticks.kvdb.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{SynchronousQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import zio.blocking.Blocking
import zio.internal.Executor
import zio.{Has, ZLayer}

object KvdbIoThreadPool {
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

  lazy val executor: Executor = zio.internal.Executor.fromThreadPoolExecutor(_ => Int.MaxValue) {
    val corePoolSize = 0
    val maxPoolSize = Int.MaxValue
    val keepAliveTime = 60000L
    val timeUnit = TimeUnit.MILLISECONDS
    val workQueue = new SynchronousQueue[Runnable]()
    val threadFactory = new KvdbIoThreadFactory("dev.chopsticks.kvdb.io", true)

    val threadPool = new ThreadPoolExecutor(
      corePoolSize,
      maxPoolSize,
      keepAliveTime,
      timeUnit,
      workQueue,
      threadFactory
    )

    threadPool
  }

  lazy val blockingEnv: ZLayer[Any, Nothing, Has[Blocking.Service]] = ZLayer.succeed {
    new Blocking.Service {
      val blockingExecutor: Executor = executor
    }
  }
}
