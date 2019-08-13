package dev.chopsticks.kvdb

import java.nio.charset.StandardCharsets

import com.google.protobuf.ByteString
import dev.chopsticks.kvdb.codec.DbKeyTransformer
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.kvdb.proto.{DbDeletePrefixRequest, DbDeleteRequest, DbPutRequest, DbTransactionAction}

import scala.collection.mutable
import scala.language.higherKinds

final class DbColumnTransactionBuilder[DbDef <: DbDefinition](val definition: DbDef) {
  private val buffer = new mutable.ListBuffer[DbTransactionAction]

  def put[CF[A, B] <: DbDef#BaseCol[A, B], K, V](column: CF[K, V], key: K, value: V): this.type = {
    val _ = buffer += DbTransactionAction(
      DbTransactionAction.Action.Put(
        DbPutRequest(
          columnId = column.id,
          key = ByteString.copyFrom(column.encodeKey(key)),
          value = ByteString.copyFrom(column.encodeValue(value))
        )
      )
    )
    this
  }

  def put[CF[A, B] <: DbDef#BaseCol[A, B], K, V](
    columnSelector: DbDef#Columns => CF[K, V],
    key: K,
    value: V
  ): this.type = {
    put(columnSelector(definition.columns), key, value)
  }

  def putValue[CF[A, B] <: DbDef#BaseCol[A, B], K, V](column: CF[K, V], value: V)(
    implicit t: DbKeyTransformer[V, K]
  ): this.type = {
    val key = t.transform(value)
    put(column, key, value)
  }

  def putValue[CF[A, B] <: DbDef#BaseCol[A, B], K, V](columnSelector: DbDef#Columns => CF[K, V], value: V)(
    implicit t: DbKeyTransformer[V, K]
  ): this.type = {
    putValue(columnSelector(definition.columns), value)
  }

  def delete[CF[A, B] <: DbDef#BaseCol[A, B], K](column: CF[K, _], key: K): this.type = {
    val _ = buffer += DbTransactionAction(
      DbTransactionAction.Action.Delete(
        DbDeleteRequest(
          columnId = column.id,
          key = ByteString.copyFrom(column.encodeKey(key))
        )
      )
    )
    this
  }

  def delete[CF[A, B] <: DbDef#BaseCol[A, B], K](columnSelector: DbDef#Columns => CF[K, _], key: K): this.type = {
    val column = columnSelector(definition.columns)
    delete(column, key)
  }

  def deletePrefix[CF[A, B] <: DbDef#BaseCol[A, B]](column: DbDef#Columns => CF[_, _], prefix: String): this.type = {
    val col = column(definition.columns)
    val _ = buffer += DbTransactionAction(
      DbTransactionAction.Action.DeletePrefix(
        DbDeletePrefixRequest(
          columnId = col.id,
          prefix = ByteString.copyFrom(prefix, StandardCharsets.UTF_8)
        )
      )
    )
    this
  }

  def result: List[DbTransactionAction] = buffer.result()
}
