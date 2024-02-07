package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.{MutationType, Transaction}
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder._
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbContext

final class FdbWriteApi[BCF[A, B] <: ColumnFamily[A, B]](
  override val tx: Transaction,
  dbContext: FdbContext[BCF],
  disableWriteConflictChecking: Boolean,
  useSnapshotReads: Boolean
) extends FdbReadApi[BCF](if (useSnapshotReads) tx.snapshot() else tx, dbContext) {
  private[chopsticks] def putByColumnId(columnId: String, key: Array[Byte], value: Array[Byte]): Unit = {
    val prefixedKey = dbContext.prefixKey(columnId, key)

    if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()

    if (dbContext.hasVersionstampKey(columnId)) {
      tx.mutate(
        MutationType.SET_VERSIONSTAMPED_KEY,
        dbContext.adjustKeyVersionstamp(columnId, prefixedKey),
        value
      )
    }
    else if (dbContext.hasVersionstampValue(columnId)) {
      tx.mutate(
        MutationType.SET_VERSIONSTAMPED_VALUE,
        prefixedKey,
        value
      )
    }
    else {
      tx.set(prefixedKey, value)
    }
  }

  private[chopsticks] def deleteByColumnId(columnId: String, key: Array[Byte]): Unit = {
    val prefixedKey = dbContext.prefixKey(columnId, key)
    if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(prefixedKey)
  }

  private[chopsticks] def deletePrefixByColumnId(columnId: String, prefix: Array[Byte]): Unit = {
    val prefixedKey = dbContext.prefixKey(columnId, prefix)
    if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(com.apple.foundationdb.Range.startsWith(prefixedKey))
  }

  private[chopsticks] def deleteRangeByColumnId(columnId: String, from: Array[Byte], to: Array[Byte]): Unit = {
    if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(dbContext.prefixKey(columnId, from), dbContext.prefixKey(columnId, to))
  }

  def put[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Unit = {
    putByColumnId(column.id, key, value)
  }

  def delete[Col <: CF](column: Col, key: Array[Byte]): Unit = {
    deleteByColumnId(column.id, key)
  }

  def deletePrefix[Col <: CF](column: Col, prefix: Array[Byte]): Unit = {
    deletePrefixByColumnId(column.id, prefix)
  }

  def deleteRangePrefix[Col <: CF](column: Col, from: Array[Byte], to: Array[Byte]): Unit = {
    deleteRangeByColumnId(column.id, from, to)
  }

  def transact(actions: Seq[TransactionWrite]): Unit = {
    actions.foreach {
      case TransactionPut(columnId, key, value) =>
        putByColumnId(columnId, key, value)

      case TransactionDelete(columnId, key) =>
        deleteByColumnId(columnId, key)

      case TransactionDeleteRange(columnId, fromKey, toKey) =>
        deleteRangeByColumnId(columnId, fromKey, toKey)

      case TransactionMutateAdd(columnId, key, value) =>
        tx.mutate(MutationType.ADD, dbContext.prefixKey(columnId, key), value)

      case TransactionMutateMin(columnId, key, value) =>
        tx.mutate(MutationType.MIN, dbContext.prefixKey(columnId, key), value)

      case TransactionMutateMax(columnId, key, value) =>
        tx.mutate(MutationType.MAX, dbContext.prefixKey(columnId, key), value)
    }
  }
}
