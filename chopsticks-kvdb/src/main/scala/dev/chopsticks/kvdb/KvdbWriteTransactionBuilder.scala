package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.codec.{KeyPrefix, KeyTransformer}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

object KvdbWriteTransactionBuilder:
  enum TransactionWrite:
    case Put(columnId: String, key: Array[Byte], value: Array[Byte])
    case Delete(columnId: String, key: Array[Byte])
    case DeleteRange(columnId: String, fromKey: Array[Byte], toKey: Array[Byte])
    case MutateAdd(columnId: String, key: Array[Byte], value: Array[Byte])

final class KvdbWriteTransactionBuilder[CFS <: ColumnFamily[_, _]]:
  import KvdbWriteTransactionBuilder._

  private val factory = new KvdbOperationFactory[CFS]
  private val buffer = new ConcurrentLinkedQueue[TransactionWrite]
  private val currentVersion = new AtomicInteger(0)

  def add(operation: TransactionWrite): this.type =
    val _ = buffer.add(operation)
    this

  def addAll(operations: Iterable[TransactionWrite]): this.type =
    operations.foreach(add)
    this

  def put[CF >: CFS <: ColumnFamily[_, _]](column: CF, key: column.Key, value: column.Value): this.type =
    add(factory.put(column, key, value))

  def putValue[CF >: CFS <: ColumnFamily[_, _]](column: CF, value: column.Value)(using
    t: KeyTransformer[column.Value, column.Key]
  ): this.type =
    add(factory.putValue(column, value))

  def delete[CF >: CFS <: ColumnFamily[_, _]](column: CF, key: column.Key): this.type =
    add(factory.delete(column, key))

  def deleteRange[CF >: CFS <: ColumnFamily[_, _]](
    column: CF,
    fromKey: column.Key,
    toKey: column.Key,
    inclusive: Boolean
  ): this.type =
    factory.deleteRange(column, fromKey, toKey, inclusive).foreach(add)
    this

  def deletePrefixRange[CF >: CFS <: ColumnFamily[_, _], FP, TP](
    column: CF,
    fromPrefix: FP,
    toPrefix: TP,
    inclusive: Boolean = false
  )(implicit
    ev1: KeyPrefix[FP, column.Key],
    ev2: KeyPrefix[TP, column.Key]
  ): this.type =
    add(factory.deletePrefixRange(column, fromPrefix, toPrefix, inclusive))

  def deletePrefix[CF >: CFS <: ColumnFamily[_, _], P](column: CF, prefix: P)(using
    ev: KeyPrefix[P, column.Key]
  ): this.type =
    add(factory.deletePrefix(column, prefix))

  def mutateAdd[CF >: CFS <: ColumnFamily[_, _]](column: CF, key: column.Key, value: column.Value): this.type =
    add(factory.mutateAdd(column, key, value))

  def nextVersion: Int = currentVersion.getAndIncrement()

  def result: List[TransactionWrite] =
    import scala.jdk.CollectionConverters._
    List.from(buffer.asScala)

end KvdbWriteTransactionBuilder
