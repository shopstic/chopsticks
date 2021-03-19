package dev.chopsticks.fp.util

import zio.internal.{ExecutionMetrics, Executor, Platform}

import java.util.concurrent.{
  LinkedBlockingQueue,
  RejectedExecutionException,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger

object PlatformUtils {
  private final class NamedThreadFactory(name: String, daemon: Boolean) extends ThreadFactory {
    private val parentGroup =
      Option(System.getSecurityManager).fold(Thread.currentThread().getThreadGroup)(_.getThreadGroup)

    private val threadGroup = new ThreadGroup(parentGroup, name)
    private val threadCount = new AtomicInteger(1)

    override def newThread(r: Runnable): Thread = {
      val newThreadNumber = threadCount.getAndIncrement()

      val thread = new Thread(threadGroup, r)
      thread.setName(s"$name-$newThreadNumber")
      thread.setDaemon(daemon)

      thread
    }
  }

  def create(
    corePoolSize: Int = 2,
    maxPoolSize: Int = 2,
    keepAliveTimeMs: Int = 15000,
    threadPoolName: String = "zio-custom-async"
  ): Platform = {
    val timeUnit = TimeUnit.MILLISECONDS
    val workQueue = new LinkedBlockingQueue[Runnable]()
    val threadFactory = new NamedThreadFactory(threadPoolName, true)

    val es = new ThreadPoolExecutor(
      corePoolSize,
      maxPoolSize,
      keepAliveTimeMs,
      timeUnit,
      workQueue,
      threadFactory
    )

    Platform.fromExecutor(new Executor {
      override val yieldOpCount: Int = Platform.defaultYieldOpCount
      override def metrics: Option[ExecutionMetrics] = None
      override def submit(runnable: Runnable): Boolean = {
        try {
          es.execute(runnable)
          true
        }
        catch {
          case _: RejectedExecutionException => false
        }
      }
    })
  }
}
