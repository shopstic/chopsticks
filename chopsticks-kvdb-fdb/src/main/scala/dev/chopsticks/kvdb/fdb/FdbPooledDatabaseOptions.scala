package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.{DatabaseOptions, OptionConsumer}

import scala.annotation.nowarn

object FdbPooledDatabaseOptions {
  final class DummyOptionConsumer extends OptionConsumer {
    override def setOption(code: Int, parameter: Array[Byte]): Unit = ???
  }
}

final class FdbPooledDatabaseOptions(pool: Iterable[DatabaseOptions])
    extends DatabaseOptions(new FdbPooledDatabaseOptions.DummyOptionConsumer) {
  override def setLocationCacheSize(value: Long): Unit = pool.foreach(_.setLocationCacheSize(value))

  override def setMaxWatches(value: Long): Unit = pool.foreach(_.setMaxWatches(value))

  override def setMachineId(value: String): Unit = pool.foreach(_.setMachineId(value))

  override def setDatacenterId(value: String): Unit = pool.foreach(_.setDatacenterId(value))

  override def setSnapshotRywEnable(): Unit = pool.foreach(_.setSnapshotRywEnable())

  override def setSnapshotRywDisable(): Unit = pool.foreach(_.setSnapshotRywDisable())

  override def setTransactionLoggingMaxFieldLength(value: Long): Unit =
    pool.foreach(_.setTransactionLoggingMaxFieldLength(value))

  override def setTransactionTimeout(value: Long): Unit = pool.foreach(_.setTransactionTimeout(value))

  override def setTransactionRetryLimit(value: Long): Unit = pool.foreach(_.setTransactionRetryLimit(value))

  override def setTransactionMaxRetryDelay(value: Long): Unit = pool.foreach(_.setTransactionMaxRetryDelay(value))

  override def setTransactionSizeLimit(value: Long): Unit = pool.foreach(_.setTransactionSizeLimit(value))

  override def setTransactionCausalReadRisky(): Unit = pool.foreach(_.setTransactionCausalReadRisky())

  @nowarn("cat=deprecation")
  override def setTransactionIncludePortInAddress(): Unit = pool.foreach(_.setTransactionIncludePortInAddress())

  override def setTransactionBypassUnreadable(): Unit = pool.foreach(_.setTransactionBypassUnreadable())

  override def setUseConfigDatabase(): Unit = pool.foreach(_.setUseConfigDatabase())

  override def setTestCausalReadRisky(): Unit = pool.foreach(_.setTestCausalReadRisky())
}
