package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.kvdb.ColumnFamily

import scala.language.higherKinds

trait RocksdbMaterialization[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] {
  def columnFamilyConfigMap: RocksdbColumnFamilyConfigMap[BCF, CF]
}
