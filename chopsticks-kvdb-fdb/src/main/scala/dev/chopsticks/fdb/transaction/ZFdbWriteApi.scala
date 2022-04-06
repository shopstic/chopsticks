package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbWriteApi

import java.util.concurrent.atomic.AtomicInteger

final class ZFdbWriteApi[BCF[A, B] <: ColumnFamily[A, B]](api: FdbWriteApi[BCF]) {
  private lazy val currentVersion = new AtomicInteger(0)

  def keyspace[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](keyspace: CF[K, V] with CF2)
    : ZFdbKeyspaceWriteApi[BCF, CF2, K, V] = {
    new ZFdbKeyspaceWriteApi(keyspace, api)
  }

  def nextVersion: Int = currentVersion.getAndIncrement()
}
