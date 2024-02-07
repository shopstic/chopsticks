package dev.chopsticks.fdb.transaction

import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}
import dev.chopsticks.kvdb.fdb.FdbWriteApi
import dev.chopsticks.kvdb.{ColumnFamily, KvdbOperationFactory}

final class ZFdbKeyspaceWriteApi[BCF[A, B] <: ColumnFamily[A, B], CF <: BCF[K, V], K, V](
  keyspace: CF,
  api: FdbWriteApi[BCF]
) extends ZFdbKeyspaceReadApi[BCF, CF, K, V](keyspace, api) {
  val operationFactory: KvdbOperationFactory[BCF] = new KvdbOperationFactory[BCF]

  def transact(operation: TransactionWrite): Unit = {
    api.transact(operation :: Nil)
  }

  def transact(operations: Seq[TransactionWrite]): Unit = {
    api.transact(operations)
  }

  def put(key: K, value: V): Unit = {
    transact(operationFactory.put(keyspace, key, value))
  }

  def putRawValue(key: K, value: Array[Byte]): Unit = {
    transact(operationFactory.putRawValue(keyspace, key, value))
  }

  def putValue(value: V)(implicit
    t: KeyTransformer[V, K]
  ): Unit = {
    transact(operationFactory.putValue(keyspace, value))
  }

  def delete(key: K): Unit = {
    transact(operationFactory.delete(keyspace, key))
  }

  def deleteRange(
    fromKey: K,
    toKey: K,
    inclusive: Boolean
  ): Unit = {
    transact(operationFactory.deleteRange(keyspace, fromKey, toKey, inclusive))
  }

  def deletePrefixRange[FP, TP](
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean = false
  )(implicit
    ev1: KeyPrefix[FP, K],
    ev2: KeyPrefix[TP, K]
  ): Unit = {
    transact(operationFactory.deletePrefixRange(keyspace, fromPrefix, toPrefix, inclusive))
  }

  def deletePrefix[P](prefix: P)(
    implicit ev: KeyPrefix[P, K]
  ): Unit = {
    transact(operationFactory.deletePrefix(keyspace, prefix))
  }

  def mutateAdd(key: K, value: V): Unit = {
    transact(operationFactory.mutateAdd(keyspace, key, value))
  }

  def mutateMin(key: K, value: V): Unit = {
    transact(operationFactory.mutateMin(keyspace, key, value))
  }

  def mutateMax(key: K, value: V): Unit = {
    transact(operationFactory.mutateMax(keyspace, key, value))
  }
}
