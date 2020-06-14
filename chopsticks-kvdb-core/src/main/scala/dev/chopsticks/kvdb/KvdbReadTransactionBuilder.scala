package dev.chopsticks.kvdb

import java.util.concurrent.ConcurrentLinkedQueue

object KvdbReadTransactionBuilder {
  final case class TransactionGet(columnId: String, key: Array[Byte])
}

final class KvdbReadTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  import KvdbReadTransactionBuilder._

  private val buffer = new ConcurrentLinkedQueue[TransactionGet]

  def get[CF <: BCF[K, _], K](column: CF, key: K): this.type = {
    val _ = buffer.add(
      TransactionGet(
        columnId = column.id,
        key = column.serializeKey(key)
      )
    )
    this
  }

  def result: List[TransactionGet] = {
    import scala.jdk.CollectionConverters._
    List.from(buffer.asScala)
  }
}
