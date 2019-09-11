package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.kvdb.ColumnFamily
import org.rocksdb.ColumnFamilyOptions

import scala.language.higherKinds

object RocksdbColumnFamilyOptionsMap {
  def apply[CF[A, B] <: ColumnFamily[A, B]] = new RocksdbColumnFamilyOptionsMap[CF, CF[_, _]](Map.empty)
}

final class RocksdbColumnFamilyOptionsMap[BCF[A, B] <: ColumnFamily[A, B], +CF <: BCF[_, _]] private (
  val map: Map[BCF[_, _], ColumnFamilyOptions]
) {
  def and[B <: BCF[_, _]](cf: B, cfg: ColumnFamilyOptions): RocksdbColumnFamilyOptionsMap[BCF, CF with B] = {
    new RocksdbColumnFamilyOptionsMap[BCF, CF with B](map.updated(cf, cfg))
  }

  def of[B <: BCF[_, _]](cf: B, cfg: ColumnFamilyOptions): RocksdbColumnFamilyOptionsMap[BCF, B] = {
    new RocksdbColumnFamilyOptionsMap[BCF, B](Map(cf -> cfg))
  }
}
