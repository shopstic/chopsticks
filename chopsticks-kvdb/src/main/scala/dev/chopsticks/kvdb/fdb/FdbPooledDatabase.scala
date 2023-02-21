package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb._

import java.util.concurrent.{CompletableFuture, Executor, ThreadLocalRandom}
import java.util.function
import scala.collection.immutable.ArraySeq

final class FdbPooledDatabase(pool: ArraySeq[Database]) extends Database:

  private def underlying: Database =
    pool(ThreadLocalRandom.current().nextInt(pool.size))

  override def openTenant(tenantName: Tuple): Tenant =
    underlying.openTenant(tenantName)

  override def openTenant(tenantName: Array[Byte], e: Executor): Tenant =
    underlying.openTenant(tenantName, e)

  override def openTenant(tenantName: Tuple, e: Executor): Tenant =
    underlying.openTenant(tenantName, e)

  override def openTenant(tenantName: Array[Byte], e: Executor, eventKeeper: EventKeeper): Tenant =
    underlying.openTenant(tenantName, e, eventKeeper)

  override def openTenant(tenantName: Tuple, e: Executor, eventKeeper: EventKeeper): Tenant =
    underlying.openTenant(tenantName, e, eventKeeper)

  override def createTransaction(e: Executor): Transaction =
    underlying.createTransaction(e)

  override def createTransaction(e: Executor, eventKeeper: EventKeeper): Transaction =
    underlying.createTransaction(e, eventKeeper)

  override def options(): DatabaseOptions =
    new FdbPooledDatabaseOptions(pool.view.map(_.options()))

  override def getMainThreadBusyness: Double =
    pool.foldLeft(0.0)((s, d) => s + d.getMainThreadBusyness)

  override def read[T](retryable: function.Function[_ >: ReadTransaction, T], e: Executor): T =
    underlying.read(retryable, e)

  override def readAsync[T](
    retryable: function.Function[_ >: ReadTransaction, _ <: CompletableFuture[T]],
    e: Executor
  ): CompletableFuture[T] =
    underlying.readAsync(retryable, e)

  override def run[T](retryable: function.Function[_ >: Transaction, T], e: Executor): T =
    underlying.run(retryable, e)

  override def runAsync[T](
    retryable: function.Function[_ >: Transaction, _ <: CompletableFuture[T]],
    e: Executor
  ): CompletableFuture[T] =
    underlying.runAsync(retryable, e)

  override def close(): Unit =
    pool.foreach(_.close())

  override def getExecutor: Executor =
    underlying.getExecutor
