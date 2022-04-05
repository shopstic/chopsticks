package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder._
import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}
import dev.chopsticks.kvdb.util.KvdbUtils

final class KvdbOperationFactory[BCF[A, B] <: ColumnFamily[A, B]] {
  def get[CF <: BCF[K, _], K](column: CF, key: K): TransactionGet = {
    TransactionGet(
      columnId = column.id,
      key = column.serializeKey(key)
    )
  }

  def put[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    column: CF[K, V] with CF2,
    key: K,
    value: V
  ): TransactionPut = {
    val (k, v) = column.serialize(key, value)
    TransactionPut(
      columnId = column.id,
      key = k,
      value = v
    )
  }

  def putRawValue[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K](
    column: CF[K, _] with CF2,
    key: K,
    value: Array[Byte]
  ): TransactionPut = {
    TransactionPut(
      columnId = column.id,
      key = column.serializeKey(key),
      value = value
    )
  }

  def putValue[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](column: CF[K, V] with CF2, value: V)(implicit
    t: KeyTransformer[V, K]
  ): TransactionPut = {
    val key = t.transform(value)
    put(column, key, value)
  }

  def delete[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K](
    column: CF[K, _] with CF2,
    key: K
  ): TransactionDelete = {
    TransactionDelete(
      columnId = column.id,
      key = column.serializeKey(key)
    )
  }

  def deleteRange[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K](
    column: CF[K, _] with CF2,
    fromKey: K,
    toKey: K,
    inclusive: Boolean
  ): List[TransactionWrite] = {
    val op1 = TransactionDeleteRange(
      columnId = column.id,
      fromKey = column.serializeKey(fromKey),
      toKey = column.serializeKey(toKey)
    )

    if (inclusive) {
      List[TransactionWrite](op1, delete(column, toKey))
    }
    else {
      List(op1)
    }
  }

  def deletePrefixRange[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, FP, TP](
    column: CF[K, _] with CF2,
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean
  )(implicit
    ev1: KeyPrefix[FP, K],
    ev2: KeyPrefix[TP, K]
  ): TransactionDeleteRange = {
    val toPrefixBytes = column.serializeKeyPrefix(toPrefix)

    TransactionDeleteRange(
      columnId = column.id,
      fromKey = column.serializeKeyPrefix(fromPrefix),
      toKey = if (inclusive) KvdbUtils.strinc(toPrefixBytes) else toPrefixBytes
    )
  }

  def deletePrefix[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, _], K, P](column: CF[K, _] with CF2, prefix: P)(
    implicit ev: KeyPrefix[P, K]
  ): TransactionDeleteRange = {
    val prefixBytes = column.serializeKeyPrefix(prefix)
    TransactionDeleteRange(
      columnId = column.id,
      fromKey = prefixBytes,
      toKey = KvdbUtils.strinc(prefixBytes)
    )
  }

  def mutateAdd[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    column: CF[K, V] with CF2,
    key: K,
    value: V
  ): TransactionMutateAdd = {
    val (k, v) = column.serialize(key, value)
    TransactionMutateAdd(
      columnId = column.id,
      key = k,
      value = v
    )
  }
}
