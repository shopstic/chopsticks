package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

object KvdbWriteTransactionBuilder {
  sealed trait TransactionWrite
  final case class TransactionPut(columnId: String, key: Array[Byte], value: Array[Byte]) extends TransactionWrite
  final case class TransactionDelete(columnId: String, key: Array[Byte]) extends TransactionWrite
  final case class TransactionDeleteRange(columnId: String, fromKey: Array[Byte], toKey: Array[Byte])
      extends TransactionWrite
  final case class TransactionMutateAdd(columnId: String, key: Array[Byte], value: Array[Byte])
      extends TransactionWrite
}

final class KvdbWriteTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  import KvdbWriteTransactionBuilder._

  private val factory = new KvdbOperationFactory[BCF]
  private val buffer = new ConcurrentLinkedQueue[TransactionWrite]
  private val currentVersion = new AtomicInteger(0)

  def add(operation: TransactionWrite): this.type = {
    val _ = buffer.add(operation)
    this
  }

  def addAll(operations: Iterable[TransactionWrite]): this.type = {
    operations.foreach(add)
    this
  }

  def put[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    column: CF[K, V] with CF2,
    key: K,
    value: V
  ): this.type = {
    add(factory.put(column, key, value))
  }

  def putValue[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](column: CF[K, V] with CF2, value: V)(implicit
    t: KeyTransformer[V, K]
  ): this.type = {
    add(factory.putValue(column, value))
  }

  def delete[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K](column: CF[K, _] with CF2, key: K): this.type = {
    add(factory.delete(column, key))
  }

  def deleteRange[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K](
    column: CF[K, _] with CF2,
    fromKey: K,
    toKey: K,
    inclusive: Boolean
  ): this.type = {
    factory.deleteRange(column, fromKey, toKey, inclusive).foreach(add)
    this
  }

  def deletePrefixRange[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, FP, TP](
    column: CF[K, _] with CF2,
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean = false
  )(implicit
    ev1: KeyPrefix[FP, K],
    ev2: KeyPrefix[TP, K]
  ): this.type = {
    add(factory.deletePrefixRange(column, fromPrefix, toPrefix, inclusive))
  }

  def deletePrefix[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, P](column: CF[K, _] with CF2, prefix: P)(
    implicit ev: KeyPrefix[P, K]
  ): this.type = {
    add(factory.deletePrefix(column, prefix))
  }

  def mutateAdd[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    column: CF[K, V] with CF2,
    key: K,
    value: V
  ): this.type = {
    add(factory.mutateAdd(column, key, value))
  }

  def nextVersion: Int = currentVersion.getAndIncrement()

  def result: List[TransactionWrite] = {
    import scala.jdk.CollectionConverters._
    List.from(buffer.asScala)
  }
}
