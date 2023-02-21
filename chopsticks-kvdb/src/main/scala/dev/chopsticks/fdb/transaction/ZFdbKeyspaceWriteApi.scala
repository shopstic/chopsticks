package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}
import dev.chopsticks.kvdb.fdb.FdbWriteApi
import dev.chopsticks.kvdb.{ColumnFamily, KvdbOperationFactory}

final class ZFdbKeyspaceWriteApi[CFS <: ColumnFamily[_, _], CF <: ColumnFamily[_, _]](
  override val keyspace: CF,
  override val api: FdbWriteApi[CFS]
)(using CFS <:< CF) extends ZFdbKeyspaceReadApi[CFS, CF](keyspace, api):
  val operationFactory: KvdbOperationFactory[CFS] = new KvdbOperationFactory[CFS]

  def transact(operation: TransactionWrite): Unit =
    api.transact(operation :: Nil)

  def transact(operations: Seq[TransactionWrite]): Unit =
    api.transact(operations)

  def put(key: keyspace.Key, value: keyspace.Value): Unit =
    transact(operationFactory.put(keyspace, key, value))

  def putRawValue(key: keyspace.Key, value: Array[Byte]): Unit =
    transact(operationFactory.putRawValue(keyspace, key, value))

  def putValue(value: keyspace.Value)(implicit
    t: KeyTransformer[keyspace.Value, keyspace.Key]
  ): Unit =
    transact(operationFactory.putValue(keyspace, value))

  def delete(key: keyspace.Key): Unit =
    transact(operationFactory.delete(keyspace, key))

  def deleteRange(
    fromKey: keyspace.Key,
    toKey: keyspace.Key,
    inclusive: Boolean
  ): Unit =
    transact(operationFactory.deleteRange(keyspace, fromKey, toKey, inclusive))

  def deletePrefixRange[FP, TP](
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean = false
  )(implicit
    ev1: KeyPrefix[FP, keyspace.Key],
    ev2: KeyPrefix[TP, keyspace.Key]
  ): Unit =
    transact(operationFactory.deletePrefixRange(keyspace, fromPrefix, toPrefix, inclusive))

  def deletePrefix[P](prefix: P)(
    implicit ev: KeyPrefix[P, keyspace.Key]
  ): Unit =
    transact(operationFactory.deletePrefix(keyspace, prefix))

  def mutateAdd(key: keyspace.Key, value: keyspace.Value): Unit =
    transact(operationFactory.mutateAdd(keyspace, key, value))

end ZFdbKeyspaceWriteApi
