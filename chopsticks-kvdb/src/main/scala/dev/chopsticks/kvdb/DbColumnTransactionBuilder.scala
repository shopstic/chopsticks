package dev.chopsticks.kvdb

import dev.chopsticks.kvdb.codec.DbKeyTransformer
import dev.chopsticks.kvdb.DbClient.{TransactionAction, TransactionDelete, TransactionDeletePrefix, TransactionPut}
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.proto.db.DbTransactionAction

import scala.collection.mutable
import scala.language.higherKinds

final class DbColumnTransactionBuilder[DbDef <: DbDefinition](val definition: DbDef) {
  type Txn = TransactionAction[DbDef#BaseCol[Any, Any]]
  private val buffer = new mutable.ListBuffer[Txn]

  def put[CF[A, B] <: DbDef#BaseCol[A, B], K, V](column: CF[K, V], key: K, value: V): this.type = {
    val _ = buffer += TransactionPut(column, key, value).asInstanceOf[Txn]
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
    val _ = buffer += TransactionDelete(column, key).asInstanceOf[Txn]
    this
  }

  def delete[CF[A, B] <: DbDef#BaseCol[A, B], K](columnSelector: DbDef#Columns => CF[K, _], key: K): this.type = {
    val column = columnSelector(definition.columns)
    delete(column, key)
  }

  def deletePrefix[CF[A, B] <: DbDef#BaseCol[A, B]](column: DbDef#Columns => CF[_, _], prefix: String): this.type = {
    val col = column(definition.columns)
    val _ = buffer += TransactionDeletePrefix(col, prefix).asInstanceOf[Txn]
    this
  }

  def result: List[Txn] = buffer.result()

  def encodedResult: List[DbTransactionAction] = result.map(v => DbClient.encodeTransaction(v))
}
