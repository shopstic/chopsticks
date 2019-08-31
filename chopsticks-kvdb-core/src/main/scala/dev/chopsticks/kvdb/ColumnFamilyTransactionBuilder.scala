package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets

import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.codec.KeyTransformer
import dev.chopsticks.kvdb.proto.{KvdbDeletePrefixRequest, KvdbDeleteRequest, KvdbPutRequest, KvdbTransactionAction}

import scala.collection.mutable
import scala.language.higherKinds
import dev.chopsticks.kvdb.util.UnusedImplicits._

final class ColumnFamilyTransactionBuilder[BaseCol <: ColumnFamily[_, _]] {
  private val buffer = new mutable.ListBuffer[KvdbTransactionAction]

  def put[CF[A, B] <: ColumnFamily[A, B], K, V](column: CF[K, V], key: K, value: V)(
    implicit ev: CF[K, V] <:< BaseCol
  ): this.type = {
    ev.unused()
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

  def putValue[CF[A, B] <: ColumnFamily[A, B], K, V](column: CF[K, V], value: V)(
    implicit t: KeyTransformer[V, K],
    ev: CF[K, V] <:< BaseCol
  ): this.type = {
    val key = t.transform(value)
    put(column, key, value)
  }

  def delete[CF[A, B] <: ColumnFamily[A, B], K](column: CF[K, _], key: K)(
    implicit ev: CF[K, _] <:< BaseCol
  ): this.type = {
    ev.unused()
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

  def deletePrefix[CF[A, B] <: ColumnFamily[A, B]](col: CF[_, _], prefix: String)(
    implicit ev: CF[_, _] <:< BaseCol
  ): this.type = {
    ev.unused()
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
