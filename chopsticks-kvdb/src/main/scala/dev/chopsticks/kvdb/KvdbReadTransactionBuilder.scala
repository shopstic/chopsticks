package dev.chopsticks.kvdb

import java.util.concurrent.ConcurrentLinkedQueue

object KvdbReadTransactionBuilder:
  enum TransactionRead:
    case Get(columnId: String, key: Array[Byte])

final class KvdbReadTransactionBuilder[CFS <: ColumnFamily[_, _]]:
  import KvdbReadTransactionBuilder._

  private val buffer = new ConcurrentLinkedQueue[TransactionRead.Get]

  def get[CF >: CFS <: ColumnFamily[_, _]](column: CF, key: column.Key): this.type =
    val _ = buffer.add(
      TransactionRead.Get(
        columnId = column.id,
        key = column.serializeKey(key)
      )
    )
    this

  def result: List[TransactionRead.Get] =
    import scala.jdk.CollectionConverters._
    List.from(buffer.asScala)
