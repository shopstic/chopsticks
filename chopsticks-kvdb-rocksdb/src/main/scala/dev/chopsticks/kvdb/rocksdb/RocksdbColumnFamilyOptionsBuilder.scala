package dev.chopsticks.kvdb.rocksdb

import org.rocksdb._
import squants.information.Mebibytes

import scala.collection.JavaConverters._

object RocksdbColumnFamilyOptionsBuilder {
  def apply(config: RocksdbColumnFamilyConfig): RocksdbColumnFamilyOptionsBuilder = {
    new RocksdbColumnFamilyOptionsBuilder(config)
  }
}

final class RocksdbColumnFamilyOptionsBuilder private (config: RocksdbColumnFamilyConfig) {
  private val memoryBudgetBytes = config.memoryBudget.toBytes.toLong
  private val blockCacheBytes = config.blockCache.toBytes.toLong

  //  private val writeBufferSize = memoryBudget / 4
  private val columnOptions = {
    val writeBufferSize = memoryBudgetBytes / 4

    val cf = new ColumnFamilyOptions()
      .setWriteBufferSize(writeBufferSize)
      .setMaxWriteBufferNumber(4)
      .setMinWriteBufferNumberToMerge(1)
      .setTargetFileSizeBase(writeBufferSize)
      .setTargetFileSizeMultiplier(10)
      .setMaxBytesForLevelBase(writeBufferSize * 10)
      .setMaxBytesForLevelMultiplier(10.0)
      .setLevel0FileNumCompactionTrigger(1)

    val numLevels = cf.numLevels()

    cf.setLevel0SlowdownWritesTrigger(Int.MaxValue)
      .setLevel0StopWritesTrigger(Int.MaxValue)
      .setSoftPendingCompactionBytesLimit(0)
      .setHardPendingCompactionBytesLimit(0)
      .setMaxCompactionBytes(Long.MaxValue)
      .setCompressionType(config.compression)
      .setCompressionPerLevel(
        ((0 to 1).map(_ => CompressionType.NO_COMPRESSION) ++ (2 to numLevels).map(_ => config.compression)).asJava
      )
      .setOptimizeFiltersForHits(true)
  }

  private def createTableFormat =
    if (blockCacheBytes > 0) {
      new BlockBasedTableConfig()
        .setBlockSize(config.blockSize.toBytes.toLong)
        // TODO: ClockCache does not work, file an issue
        .setBlockCache(new LRUCache(blockCacheBytes))
//        .setCacheIndexAndFilterBlocks(true)
//        .setCacheIndexAndFilterBlocksWithHighPriority(true)
//        .setPinL0FilterAndIndexBlocksInCache(true)
    }
    else {
      new BlockBasedTableConfig()
        .setBlockSize(config.blockSize.toBytes.toLong)
        .setNoBlockCache(true)
    }

  def withTotalOrder(): RocksdbColumnFamilyOptionsBuilder = {
    val _ = columnOptions
      .setTableFormatConfig(createTableFormat)
    this
  }

  def withCappedPrefixExtractor(length: Int): RocksdbColumnFamilyOptionsBuilder = {
    val tableFormat = createTableFormat
      .setIndexType(IndexType.kHashSearch)

    {
      val _ = columnOptions
        .useCappedPrefixExtractor(length)
        .setTableFormatConfig(tableFormat)
    }
    this
  }

  def withPointLookup(): RocksdbColumnFamilyOptionsBuilder = {
    {
      val _ = columnOptions
        .optimizeForPointLookup(config.blockCache.to(Mebibytes).toLong)
    }
    this
  }

  def withMergeOperatorName(name: String): RocksdbColumnFamilyOptionsBuilder = {
    val _ = columnOptions.setMergeOperatorName(name)
    this
  }

  def build(): ColumnFamilyOptions = {
    columnOptions
  }
}
