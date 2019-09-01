package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.kvdb.ColumnFamily
import eu.timepit.refined.types.numeric.PosInt
import org.rocksdb.CompressionType
import squants.information.Information

object RocksdbColumnFamilyConfig {
  sealed trait ReadPattern

  object PointLookupPattern extends ReadPattern
  object TotalOrderScanPattern extends ReadPattern
  final case class PrefixedScanPattern(minPrefixLength: PosInt) extends ReadPattern

}

final case class RocksdbColumnFamilyConfig[+CF <: ColumnFamily[_, _]](
  memoryBudget: Information,
  blockCache: Information,
  blockSize: Information,
  readPattern: RocksdbColumnFamilyConfig.ReadPattern,
  compression: CompressionType = CompressionType.NO_COMPRESSION
)
