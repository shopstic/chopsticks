package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbWriteApi

import java.util.concurrent.atomic.AtomicInteger

final class ZFdbWriteApi[CFS <: ColumnFamily[_, _]](api: FdbWriteApi[CFS]):
  private lazy val currentVersion = new AtomicInteger(0)

  def keyspace[CF <: ColumnFamily[_, _]](keyspace: CF)(using CFS <:< CF): ZFdbKeyspaceWriteApi[CFS, CF] =
    new ZFdbKeyspaceWriteApi(keyspace, api)

  def nextVersion: Int = currentVersion.getAndIncrement()

end ZFdbWriteApi
