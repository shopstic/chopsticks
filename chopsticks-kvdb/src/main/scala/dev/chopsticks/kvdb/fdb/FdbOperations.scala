package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.async.AsyncIterator
import dev.chopsticks.kvdb.ColumnFamily

import java.util.concurrent.CompletableFuture

trait FdbOperations[CFS <: ColumnFamily[_, _]]:
  def read[V](api: FdbReadApi[CFS], fn: FdbReadApi[CFS] => CompletableFuture[V]): CompletableFuture[V]
  def write[V](api: FdbWriteApi[CFS], fn: FdbWriteApi[CFS] => CompletableFuture[V]): CompletableFuture[V]
  def iterate(api: FdbReadApi[CFS], fn: FdbReadApi[CFS] => AsyncIterator[KeyValue]): AsyncIterator[KeyValue]

final class FdbDefaultOperations[CFS <: ColumnFamily[_, _]] extends FdbOperations[CFS]:
  override def read[V](api: FdbReadApi[CFS], fn: FdbReadApi[CFS] => CompletableFuture[V]): CompletableFuture[V] =
    fn(api)

  override def write[V](api: FdbWriteApi[CFS], fn: FdbWriteApi[CFS] => CompletableFuture[V]): CompletableFuture[V] =
    fn(api)

  override def iterate(
    api: FdbReadApi[CFS],
    fn: FdbReadApi[CFS] => AsyncIterator[KeyValue]
  ): AsyncIterator[KeyValue] =
    fn(api)
