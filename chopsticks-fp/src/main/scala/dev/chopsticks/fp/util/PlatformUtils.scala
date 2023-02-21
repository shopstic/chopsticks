package dev.chopsticks.fp.util

import java.util.concurrent.{LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object PlatformUtils:

  final private class NamedThreadFactory(name: String, daemon: Boolean) extends ThreadFactory:
    private val parentGroup = Thread.currentThread().getThreadGroup

    private val threadGroup = new ThreadGroup(parentGroup, name)
    private val threadCount = new AtomicInteger(1)

    override def newThread(r: Runnable): Thread =
      val newThreadNumber = threadCount.getAndIncrement()
      val thread = new Thread(threadGroup, r)
      thread.setName(s"$name-$newThreadNumber")
      thread.setDaemon(daemon)
      thread
  end NamedThreadFactory

  def createExecutor(
    corePoolSize: Int = 2,
    maxPoolSize: Int = 2,
    keepAliveTimeMs: Int = 15000,
    threadPoolName: String = "zio-custom-async"
  ): zio.Executor =
    zio.Executor.fromThreadPoolExecutor(
      new ThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        keepAliveTimeMs,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable](),
        new NamedThreadFactory(threadPoolName, true)
      )
    )

end PlatformUtils
