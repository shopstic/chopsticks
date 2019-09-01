package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.kvdb.ColumnFamily

import scala.language.higherKinds

object RocksdbColumnFamilyConfigMap {
  def apply[CF[A, B] <: ColumnFamily[A, B]] = new RocksdbColumnFamilyConfigMap[CF, CF[_, _]](Map.empty)
}

final class RocksdbColumnFamilyConfigMap[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] private (
  val map: Map[BCF[_, _], RocksdbColumnFamilyConfig[BCF[_, _]]]
) {
  def and[B <: BCF[_, _]](cf: B, cfg: RocksdbColumnFamilyConfig[B]): RocksdbColumnFamilyConfigMap[BCF, CF with B] = {
    new RocksdbColumnFamilyConfigMap[BCF, CF with B](map.updated(cf, cfg))
  }

  def of[B <: BCF[_, _]](cf: B, cfg: RocksdbColumnFamilyConfig[B]): RocksdbColumnFamilyConfigMap[BCF, B] = {
    new RocksdbColumnFamilyConfigMap[BCF, B](Map(cf -> cfg))
  }
}
