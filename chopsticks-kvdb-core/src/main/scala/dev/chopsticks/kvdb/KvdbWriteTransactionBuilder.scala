package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.codec.KeyTransformer

import scala.collection.mutable

object KvdbWriteTransactionBuilder {
  sealed trait TransactionWrite
  final case class TransactionPut(columnId: String, key: Array[Byte], value: Array[Byte]) extends TransactionWrite
  final case class TransactionDelete(columnId: String, key: Array[Byte]) extends TransactionWrite
  final case class TransactionDeleteRange(columnId: String, fromKey: Array[Byte], toKey: Array[Byte])
      extends TransactionWrite
}

final class KvdbWriteTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  import KvdbWriteTransactionBuilder._

  private val buffer = new mutable.ListBuffer[TransactionWrite]

  def put[CF <: BCF[K, V], K, V](column: CF, key: K, value: V): this.type = {
    val (k, v) = column.serialize(key, value)
    val _ = buffer += TransactionPut(
      columnId = column.id,
      key = k,
      value = v
    )
    this
  }

  def putValue[CF <: BCF[K, V], K, V](column: CF, value: V)(
    implicit t: KeyTransformer[V, K]
  ): this.type = {
    val key = t.transform(value)
    put(column, key, value)
  }

  def delete[CF <: BCF[K, _], K](column: CF, key: K): this.type = {
    val _ = buffer += TransactionDelete(
      columnId = column.id,
      key = column.serializeKey(key)
    )
    this
  }

  def deleteRange[CF <: BCF[K, _], K](column: CF, fromKey: K, toKey: K): this.type = {
    val _ = buffer += TransactionDeleteRange(
      columnId = column.id,
      fromKey = column.serializeKey(fromKey),
      toKey = column.serializeKey(toKey)
    )
    this
  }

  def result: List[TransactionWrite] = buffer.result()
}
