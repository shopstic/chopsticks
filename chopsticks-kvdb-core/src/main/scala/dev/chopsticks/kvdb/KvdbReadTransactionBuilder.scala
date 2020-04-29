package dev.chopsticks.kvdb

import scala.collection.mutable

object KvdbReadTransactionBuilder {
  final case class TransactionGet(columnId: String, key: Array[Byte])
}

final class KvdbReadTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  import KvdbReadTransactionBuilder._

  private val buffer = new mutable.ListBuffer[TransactionGet]

  def get[CF <: BCF[K, _], K](column: CF, key: K): this.type = {
    val _ = buffer += TransactionGet(
      columnId = column.id,
      key = column.serializeKey(key)
    )
    this
  }

  def result: List[TransactionGet] = buffer.result()
}
