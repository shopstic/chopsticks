package dev.chopsticks.util

import org.rocksdb._
import squants.information.InformationConversions._

import scala.collection.JavaConverters._

object RocksdbCFBuilder {
  final case class RocksdbCFOptions(memoryBudget: Long, blockCache: Long, minPrefixLength: Int)

  def apply(memoryBudget: Long, blockCache: Long): RocksdbCFBuilder = {
    new RocksdbCFBuilder(memoryBudget, blockCache)
  }
}

class RocksdbCFBuilder(memoryBudget: Long, blockCache: Long) {
  private val MEMTABLE_PREFIX_BLOOM_SIZE_RATIO = 0.1

  //  private val writeBufferSize = memoryBudget / 4
  private val columnOptions = {
    val writeBufferSize = memoryBudget / 4

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
      .setCompressionType(CompressionType.NO_COMPRESSION)
      .setCompressionPerLevel((0 to numLevels).map(_ => CompressionType.NO_COMPRESSION).asJava)
  }

  private val tableFormat = if (blockCache > 0) {
    new BlockBasedTableConfig()
      .setBlockSize(128.kib.toBytes.toLong)
      .setBlockCache(new ClockCache(blockCache))
      .setCacheIndexAndFilterBlocks(true)
      .setPinL0FilterAndIndexBlocksInCache(true)
  }
  else {
    new BlockBasedTableConfig()
      .setBlockSize(128.kib.toBytes.toLong)
      .setNoBlockCache(true)
  }

  def withCappedPrefixExtractor(length: Int): RocksdbCFBuilder = {
    {
      val _ = columnOptions.useCappedPrefixExtractor(length)
    }
    val _ = tableFormat.setIndexType(IndexType.kHashSearch)
    this
  }

  def withPointLookup(): RocksdbCFBuilder = {
    {
      val _ = columnOptions
        .optimizeForPointLookup(1)
        .setMemtablePrefixBloomSizeRatio(MEMTABLE_PREFIX_BLOOM_SIZE_RATIO)
    }
    val _ = tableFormat.setIndexType(IndexType.kHashSearch)
    this
  }

  def withMergeOperatorName(name: String): RocksdbCFBuilder = {
    val _ = columnOptions.setMergeOperatorName(name)
    this
  }

  def build(): ColumnFamilyOptions = {
    columnOptions.setTableFormatConfig(tableFormat)
  }
}
