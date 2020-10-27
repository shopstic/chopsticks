package dev.chopsticks.kvdb

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import dev.chopsticks.kvdb.codec.{KeyPrefixEvidence, KeyTransformer}
import dev.chopsticks.kvdb.util.KvdbUtils

object KvdbWriteTransactionBuilder {
  sealed trait TransactionWrite
  final case class TransactionPut(columnId: String, key: Array[Byte], value: Array[Byte]) extends TransactionWrite
  final case class TransactionDelete(columnId: String, key: Array[Byte]) extends TransactionWrite
  final case class TransactionDeleteRange(columnId: String, fromKey: Array[Byte], toKey: Array[Byte])
      extends TransactionWrite
}

final class KvdbWriteTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  import KvdbWriteTransactionBuilder._

  private val buffer = new ConcurrentLinkedQueue[TransactionWrite]
  private val currentVersion = new AtomicInteger(0)

  def put[CF <: BCF[K, V], K, V](column: CF, key: K, value: V): this.type = {
    val (k, v) = column.serialize(key, value)
    val _ = buffer.add(
      TransactionPut(
        columnId = column.id,
        key = k,
        value = v
      )
    )
    this
  }

  def putValue[CF <: BCF[K, V], K, V](column: CF, value: V)(implicit
    t: KeyTransformer[V, K]
  ): this.type = {
    val key = t.transform(value)
    put(column, key, value)
  }

  def delete[CF <: BCF[K, _], K](column: CF, key: K): this.type = {
    val _ = buffer.add(
      TransactionDelete(
        columnId = column.id,
        key = column.serializeKey(key)
      )
    )
    this
  }

  def deleteRange[CF <: BCF[K, _], K](column: CF, fromKey: K, toKey: K, inclusive: Boolean): this.type = {
    val _ = buffer.add(
      TransactionDeleteRange(
        columnId = column.id,
        fromKey = column.serializeKey(fromKey),
        toKey = column.serializeKey(toKey)
      )
    )

    if (inclusive) {
      delete(column, toKey)
    }
    else {
      this
    }
  }

  def deletePrefixRange[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, FP, TP](
    column: CF[K, _] with CF2,
    fromPrefix: FP,
    toPrefix: TP
  )(implicit
    ev1: KeyPrefixEvidence[FP, K],
    ev2: KeyPrefixEvidence[TP, K]
  ): KvdbWriteTransactionBuilder[BCF] = {
    val _ = buffer.add(
      TransactionDeleteRange(
        columnId = column.id,
        fromKey = column.serializeKeyPrefix(fromPrefix),
        toKey = column.serializeKeyPrefix(toPrefix)
      )
    )
    this
  }

  def deletePrefix[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, P](column: CF[K, _] with CF2, prefix: P)(
    implicit ev: KeyPrefixEvidence[P, K]
  ): KvdbWriteTransactionBuilder[BCF] = {
    val prefixBytes = column.serializeKeyPrefix(prefix)
    val _ = buffer.add(
      TransactionDeleteRange(
        columnId = column.id,
        fromKey = prefixBytes,
        toKey = KvdbUtils.strinc(prefixBytes)
      )
    )
    this
  }

  def nextVersion: Int = currentVersion.getAndIncrement()

  def result: List[TransactionWrite] = {
    import scala.jdk.CollectionConverters._
    List.from(buffer.asScala)
  }
}
