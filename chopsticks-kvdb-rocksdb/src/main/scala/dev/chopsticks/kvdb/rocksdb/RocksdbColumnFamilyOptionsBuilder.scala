package dev.chopsticks.kvdb.rocksdb

import org.rocksdb._
import squants.information.Information

import scala.collection.JavaConverters._

object RocksdbColumnFamilyOptionsBuilder {
  def apply(
    memoryBudget: Information,
    blockCache: Information,
    blockSize: Information,
    compression: CompressionType
  ): RocksdbColumnFamilyOptionsBuilder = {
    new RocksdbColumnFamilyOptionsBuilder(memoryBudget, blockCache, blockSize, compression)
  }
}

final class RocksdbColumnFamilyOptionsBuilder private (
  memoryBudget: Information,
  blockCache: Information,
  blockSize: Information,
  compression: CompressionType
) {
  private val memoryBudgetBytes = memoryBudget.toBytes.toLong
  private val blockCacheBytes = blockCache.toBytes.toLong

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
      .setCompressionType(compression)
      .setCompressionPerLevel(
        ((0 to 1).map(_ => CompressionType.NO_COMPRESSION) ++ (2 to numLevels).map(_ => compression)).asJava
      )
  }

  private val tableFormat = if (blockCacheBytes > 0) {
    new BlockBasedTableConfig()
      .setBlockSize(blockSize.toBytes.toLong)
      // TODO: ClockCache does not work, file an issue
      .setBlockCache(new LRUCache(blockCacheBytes, -1, true, 0.5))
      .setCacheIndexAndFilterBlocks(true)
      .setCacheIndexAndFilterBlocksWithHighPriority(true)
      .setPinL0FilterAndIndexBlocksInCache(true)
  }
  else {
    new BlockBasedTableConfig()
      .setBlockSize(blockSize.toBytes.toLong)
      .setNoBlockCache(true)
  }

  def withCappedPrefixExtractor(length: Int): RocksdbColumnFamilyOptionsBuilder = {
    {
      val _ = columnOptions.useCappedPrefixExtractor(length)
    }
    val _ = tableFormat
      .setIndexType(IndexType.kHashSearch)
    this
  }

  def withPointLookup(): RocksdbColumnFamilyOptionsBuilder = {
    {
      val _ = columnOptions
        .optimizeForPointLookup(1)
    }
    val _ = tableFormat
      .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
      .setDataBlockHashTableUtilRatio(0.75)
      .setFilterPolicy(new BloomFilter(10))
    this
  }

  def withMergeOperatorName(name: String): RocksdbColumnFamilyOptionsBuilder = {
    val _ = columnOptions.setMergeOperatorName(name)
    this
  }

  def build(): ColumnFamilyOptions = {
    columnOptions.setTableFormatConfig(tableFormat)
  }
}
