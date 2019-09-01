package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets

import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.codec.KeyTransformer
import dev.chopsticks.kvdb.proto.{KvdbDeletePrefixRequest, KvdbDeleteRequest, KvdbPutRequest, KvdbTransactionAction}

import scala.collection.mutable
import scala.language.higherKinds

final class ColumnFamilyTransactionBuilder[BCF[A, B] <: ColumnFamily[A, B]] {
  private val buffer = new mutable.ListBuffer[KvdbTransactionAction]

  def put[CF <: BCF[K, V], K, V](column: CF, key: K, value: V): this.type = {
    val _ = buffer += KvdbTransactionAction(
      KvdbTransactionAction.Action.Put(
        KvdbPutRequest(
          columnId = column.id,
          key = ByteString.copyFrom(column.serializeKey(key)),
          value = ByteString.copyFrom(column.serializeValue(value))
        )
      )
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
    val _ = buffer += KvdbTransactionAction(
      KvdbTransactionAction.Action.Delete(
        KvdbDeleteRequest(
          columnId = column.id,
          key = ByteString.copyFrom(column.serializeKey(key))
        )
      )
    )
    this
  }

  def deletePrefix[CF <: BCF[_, _]](col: CF, prefix: String): this.type = {
    val _ = buffer += KvdbTransactionAction(
      KvdbTransactionAction.Action.DeletePrefix(
        KvdbDeletePrefixRequest(
          columnId = col.id,
          prefix = ByteString.copyFrom(prefix, StandardCharsets.UTF_8)
        )
      )
    )
    this
  }

  def result: List[KvdbTransactionAction] = buffer.result()
}
