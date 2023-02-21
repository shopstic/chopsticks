package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.fdb.FdbReadApi

final class ZFdbReadApi[CFS <: ColumnFamily[_, _]](api: FdbReadApi[CFS]):
  def keyspace[CF <: ColumnFamily[_, _]](keyspace: CF)(using CFS <:< CF): ZFdbKeyspaceReadApi[CFS, CF] =
    new ZFdbKeyspaceReadApi(keyspace, api)
