package dev.chopsticks.fdb.zio

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbReadApi

final class ZFdbReadApi[BCF[A, B] <: ColumnFamily[A, B]](api: FdbReadApi[BCF]) {
  def keyspace[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](keyspace: CF[K, V] with CF2)
    : ZFdbKeyspaceReadApi[BCF, CF2, K, V] = {
    new ZFdbKeyspaceReadApi(keyspace, api)
  }
}
