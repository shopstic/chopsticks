package dev.chopsticks.kvdb.fdb

import com.apple.foundationdb.{MutationType, Transaction}
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.*
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbContext

import java.util.concurrent.CompletableFuture

final class FdbWriteApi[CFS <: ColumnFamily[_, _]](
  override val tx: Transaction,
  dbContext: FdbContext[CFS],
  disableWriteConflictChecking: Boolean,
  useSnapshotReads: Boolean
) extends FdbReadApi[CFS](if (useSnapshotReads) tx.snapshot() else tx, dbContext):
  private[chopsticks] def putByColumnId(columnId: String, key: Array[Byte], value: Array[Byte]): Unit =
    val prefixedKey = dbContext.prefixKey(columnId, key)

    if (disableWriteConflictChecking)
      tx.options().setNextWriteNoWriteConflictRange()

    if (dbContext.hasVersionstampKey(columnId))
      tx.mutate(
        MutationType.SET_VERSIONSTAMPED_KEY,
        dbContext.adjustKeyVersionstamp(columnId, prefixedKey),
        value
      )
    else if (dbContext.hasVersionstampValue(columnId))
      tx.mutate(
        MutationType.SET_VERSIONSTAMPED_VALUE,
        prefixedKey,
        value
      )
    else
      tx.set(prefixedKey, value)
  end putByColumnId

  private[chopsticks] def deleteByColumnId(columnId: String, key: Array[Byte]): Unit =
    val prefixedKey = dbContext.prefixKey(columnId, key)
    if (disableWriteConflictChecking)
      tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(prefixedKey)

  private[chopsticks] def deletePrefixByColumnId(columnId: String, prefix: Array[Byte]): Unit =
    val prefixedKey = dbContext.prefixKey(columnId, prefix)
    if (disableWriteConflictChecking)
      tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(com.apple.foundationdb.Range.startsWith(prefixedKey))

  private[chopsticks] def deleteRangeByColumnId(columnId: String, from: Array[Byte], to: Array[Byte]): Unit =
    if (disableWriteConflictChecking)
      tx.options().setNextWriteNoWriteConflictRange()
    tx.clear(dbContext.prefixKey(columnId, from), dbContext.prefixKey(columnId, to))

  def put[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte], value: Array[Byte])(using CFS <:< Col): Unit =
    putByColumnId(column.id, key, value)

  def delete[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte])(using CFS <:< Col): Unit =
    deleteByColumnId(column.id, key)

  def deletePrefix[Col <: ColumnFamily[_, _]](column: Col, prefix: Array[Byte])(using CFS <:< Col): Unit =
    deletePrefixByColumnId(column.id, prefix)

  def deleteRangePrefix[Col <: ColumnFamily[_, _]](column: Col, from: Array[Byte], to: Array[Byte])(using
    CFS <:< Col
  ): Unit =
    deleteRangeByColumnId(column.id, from, to)

  def watch[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte])(using CFS <:< Col): CompletableFuture[Unit] =
    val prefixedKey = dbContext.prefixKey(column.id, key)
    tx.watch(prefixedKey).thenApply(_ => ())

  def transact(actions: Seq[TransactionWrite]): Unit =
    actions.foreach {
      case TransactionWrite.Put(columnId, key, value) =>
        putByColumnId(columnId, key, value)

      case TransactionWrite.Delete(columnId, key) =>
        deleteByColumnId(columnId, key)

      case TransactionWrite.DeleteRange(columnId, fromKey, toKey) =>
        deleteRangeByColumnId(columnId, fromKey, toKey)

      case TransactionWrite.MutateAdd(columnId, key, value) =>
        tx.mutate(MutationType.ADD, dbContext.prefixKey(columnId, key), value)
    }

end FdbWriteApi
