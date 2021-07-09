package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.async.AsyncIterator
import dev.chopsticks.kvdb.ColumnFamily

import java.util.concurrent.CompletableFuture

trait FdbOperations[BCF[A, B] <: ColumnFamily[A, B]] {
  def read[V](api: FdbReadApi[BCF], fn: FdbReadApi[BCF] => CompletableFuture[V]): CompletableFuture[V]
  def write[V](api: FdbWriteApi[BCF], fn: FdbWriteApi[BCF] => CompletableFuture[V]): CompletableFuture[V]
  def iterate(api: FdbReadApi[BCF], fn: FdbReadApi[BCF] => AsyncIterator[KeyValue]): AsyncIterator[KeyValue]
}

final class FdbDefaultOperations[BCF[A, B] <: ColumnFamily[A, B]] extends FdbOperations[BCF] {
  override def read[V](api: FdbReadApi[BCF], fn: FdbReadApi[BCF] => CompletableFuture[V]): CompletableFuture[V] = {
    fn(api)
  }

  override def write[V](api: FdbWriteApi[BCF], fn: FdbWriteApi[BCF] => CompletableFuture[V]): CompletableFuture[V] = {
    fn(api)
  }

  override def iterate(
    api: FdbReadApi[BCF],
    fn: FdbReadApi[BCF] => AsyncIterator[KeyValue]
  ): AsyncIterator[KeyValue] = {
    fn(api)
  }
}
