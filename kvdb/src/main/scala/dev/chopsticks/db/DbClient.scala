package dev.chopsticks.db

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.google.protobuf.ByteString
import dev.chopsticks.db.DbInterface._
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.proto.db.{DbDeletePrefixRequest, DbDeleteRequest, DbPutRequest, DbTransactionAction}
import scalaz.zio.blocking.Blocking
import scalaz.zio.clock.Clock
import scalaz.zio.{Task, TaskR}

import scala.concurrent.Future
import scala.language.higherKinds

object DbClient {
  sealed trait TransactionAction[+CF <: DbColumn[_, _]] {
    val column: CF
  }

  final case class TransactionPut[+CF <: DbColumn[K, V], K, V](column: CF, key: K, value: V)
      extends TransactionAction[CF]

  final case class TransactionDelete[+CF <: DbColumn[K, _], K](column: CF, key: K) extends TransactionAction[CF]

  final case class TransactionDeletePrefix[+CF <: DbColumn[_, _]](column: CF, prefix: String)
      extends TransactionAction[CF]

  final case class Transaction[KV <: DbColumn[K, V], K, V](kv: KV, k: K, v: V)

  def encodeTransaction[CF[A, B] <: DbColumn[A, B], K, V](txn: TransactionAction[CF[K, V]]): DbTransactionAction = {
    txn match {
      case TransactionPut(column, key, value) =>
        DbTransactionAction(
          DbTransactionAction.Action.Put(
            DbPutRequest(
              columnId = column.id,
              key = ByteString.copyFrom(column.encodeKey(key.asInstanceOf[K])),
              value = ByteString.copyFrom(column.encodeValue(value.asInstanceOf[V]))
            )
          )
        )

      case TransactionDelete(column, key) =>
        DbTransactionAction(
          DbTransactionAction.Action.Delete(
            DbDeleteRequest(
              columnId = column.id,
              key = ByteString.copyFrom(column.encodeKey(key.asInstanceOf[K]))
            )
          )
        )

      case TransactionDeletePrefix(column, prefix) =>
        DbTransactionAction(
          DbTransactionAction.Action.DeletePrefix(
            DbDeletePrefixRequest(
              columnId = column.id,
              prefix = ByteString.copyFrom(prefix, StandardCharsets.UTF_8)
            )
          )
        )
    }
  }

  def apply[DbDef <: DbDefinition](
    db: DbInterface[DbDef]
  )(implicit akkaEnv: AkkaEnv): DbClient[DbDef] =
    new DbClient[DbDef](db)

  /*private def legacyRange(initialPrefix: String, subsequentPrefix: String, seekForPrev: Boolean): DbKeyRange = {
    if (seekForPrev) {
      DbKeyConstraints.range[String](_ <= initialPrefix ^= initialPrefix, _ ^= subsequentPrefix)
    }
    else {
      DbKeyConstraints.range[String](_ ^= initialPrefix, _ ^= subsequentPrefix)
    }
  }*/
}

final class DbClient[DbDef <: DbDefinition] private (val db: DbInterface[DbDef])(
  implicit akkaEnv: AkkaEnv
) {

  import DbClient._
  import akkaEnv._

  type Txn = TransactionAction[DbDef#BaseCol[Any, Any]]

  def column[Col[A, B] <: DbDef#BaseCol[A, B], K, V](
    col: DbDef#Columns => Col[K, V]
  ): DbColumnClient[DbDef, Col, K, V] = {
    new DbColumnClient[DbDef, Col, K, V](db, col(db.definition.columns))
  }

  def column[Col[A, B] <: DbDef#BaseCol[A, B], K, V](col: Col[K, V]): DbColumnClient[DbDef, Col, K, V] = {
    new DbColumnClient[DbDef, Col, K, V](db, col)
  }

  /*def putTransactionFlow[Col <: DbDef#BaseCol[K, V], K, V](col: DbDef#Columns => Col): Flow[(K, V), TransactionPut[Col, K, V], NotUsed] = {
    val c: Col = col(db.definition.columns)

    Flow[(K, V)]
      .map {
        case (k, v) =>
          TransactionPut(c, k, v)
      }
  }*/

  def open(): Task[this.type] = {
    db.openTask()
      .map(_ => this)
  }

  def statsTask: Task[Map[String, Double]] = db.statsTask

  def closeTask(): TaskR[Blocking with Clock, Unit] = db.closeTask()

  def compactTask(): Task[Unit] = db.compactTask()

  def transactionTask(actions: Seq[Txn]): Task[Seq[Txn]] = {
    Task {
      actions.map { a =>
        encodeTransaction(a)
      }
    }.flatMap(db.transactionTask)
      .map(_ => actions)
  }

  def transactionFlow(parallelism: Int = 1): Flow[Seq[Txn], Seq[Txn], NotUsed] = {
    Flow[Seq[Txn]]
      .mapAsync(parallelism) { b =>
        Future((b, b.map(a => encodeTransaction(a))))
      }
      .mapAsync(parallelism) {
        case (b, encoded) =>
          unsafeRunToFuture(
            db.transactionTask(encoded)
              .map(_ => b)
          )
      }
  }

  def transactionBuilder: DbColumnTransactionBuilder[DbDef] = {
    new DbColumnTransactionBuilder[DbDef](db.definition)
  }

  def startBulkInsertsTask(): Task[Unit] = {
    db.startBulkInsertsTask()
  }

  def endBulkInsertsTask(): Task[Unit] = {
    db.endBulkInsertsTask()
  }
}
