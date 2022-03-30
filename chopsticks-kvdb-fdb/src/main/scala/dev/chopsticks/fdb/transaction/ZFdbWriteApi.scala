package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbWriteApi

final class ZFdbWriteApi[BCF[A, B] <: ColumnFamily[A, B]](api: FdbWriteApi[BCF]) {
  def keyspace[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](keyspace: CF[K, V] with CF2)
    : ZFdbKeyspaceWriteApi[BCF, CF2, K, V] = {
    new ZFdbKeyspaceWriteApi(keyspace, api)
  }
}
