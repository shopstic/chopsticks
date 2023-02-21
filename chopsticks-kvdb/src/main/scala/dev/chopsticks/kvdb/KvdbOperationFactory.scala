package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.*
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.*
import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}
import dev.chopsticks.kvdb.util.KvdbUtils

final class KvdbOperationFactory[CFS <: ColumnFamily[_, _]]:
  def get[CF <: ColumnFamily[_, _]](column: CF, key: column.Key)(using CFS <:< CF): TransactionRead.Get =
    TransactionRead.Get(
      columnId = column.id,
      key = column.serializeKey(key)
    )

  def put[CF <: ColumnFamily[_, _]](
    column: CF,
    key: column.Key,
    value: column.Value
  )(using CFS <:< CF): TransactionWrite.Put =
    val (k, v) = column.serialize(key, value)
    TransactionWrite.Put(
      columnId = column.id,
      key = k,
      value = v
    )

  def putRawValue[CF <: ColumnFamily[_, _]](
    column: CF,
    key: column.Key,
    value: Array[Byte]
  )(using CFS <:< CF): TransactionWrite.Put =
    TransactionWrite.Put(
      columnId = column.id,
      key = column.serializeKey(key),
      value = value
    )

  def putValue[CF <: ColumnFamily[_, _]](column: CF, value: column.Value)(using CFS <:< CF)(using
    t: KeyTransformer[column.Value, column.Key]
  ): TransactionWrite.Put =
    val key = t.transform(value)
    put(column, key, value)

  def delete[CF <: ColumnFamily[_, _]](column: CF, key: column.Key)(using CFS <:< CF): TransactionWrite.Delete =
    TransactionWrite.Delete(
      columnId = column.id,
      key = column.serializeKey(key)
    )

  def deleteRange[CF <: ColumnFamily[_, _]](
    column: CF,
    fromKey: column.Key,
    toKey: column.Key,
    inclusive: Boolean
  )(using CFS <:< CF): List[TransactionWrite] =
    val op1 = TransactionWrite.DeleteRange(
      columnId = column.id,
      fromKey = column.serializeKey(fromKey),
      toKey = column.serializeKey(toKey)
    )
    if (inclusive) List[TransactionWrite](op1, delete(column, toKey))
    else List(op1)

  def deletePrefixRange[CF <: ColumnFamily[_, _], FP, TP](
    column: CF,
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean
  )(using CFS <:< CF)(using
    ev1: KeyPrefix[FP, column.Key],
    ev2: KeyPrefix[TP, column.Key]
  ): TransactionWrite.DeleteRange =
    val toPrefixBytes = column.serializeKeyPrefix(toPrefix)
    TransactionWrite.DeleteRange(
      columnId = column.id,
      fromKey = column.serializeKeyPrefix(fromPrefix),
      toKey = if (inclusive) KvdbUtils.strinc(toPrefixBytes) else toPrefixBytes
    )

  def deletePrefix[CF <: ColumnFamily[_, _], P](column: CF, prefix: P)(using CFS <:< CF)(
    using ev: KeyPrefix[P, column.Key]
  ): TransactionWrite.DeleteRange =
    val prefixBytes = column.serializeKeyPrefix(prefix)
    TransactionWrite.DeleteRange(
      columnId = column.id,
      fromKey = prefixBytes,
      toKey = KvdbUtils.strinc(prefixBytes)
    )

  def mutateAdd[CF <: ColumnFamily[_, _]](
    column: CF,
    key: column.Key,
    value: column.Value
  )(using CFS <:< CF): TransactionWrite.MutateAdd =
    val (k, v) = column.serialize(key, value)
    TransactionWrite.MutateAdd(
      columnId = column.id,
      key = k,
      value = v
    )

end KvdbOperationFactory
