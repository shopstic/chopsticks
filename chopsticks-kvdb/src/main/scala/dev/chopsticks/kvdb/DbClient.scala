package dev.chopsticks.kvdb

import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.DbInterface._
import dev.chopsticks.kvdb.proto.DbTransactionAction
import zio.clock.Clock
import zio.{Task, RIO}

import scala.language.higherKinds

object DbClient {
//  sealed trait TransactionAction[+CF <: DbColumn[_, _]] {
//    val column: CF
//  }
//
//  final case class TransactionPut[+CF <: DbColumn[K, V], K, V](column: CF, key: K, value: V)
//      extends TransactionAction[CF]
//
//  final case class TransactionDelete[+CF <: DbColumn[K, _], K](column: CF, key: K) extends TransactionAction[CF]
//
//  final case class TransactionDeletePrefix[+CF <: DbColumn[_, _]](column: CF, prefix: String)
//      extends TransactionAction[CF]
//
//  final case class Transaction[KV <: DbColumn[K, V], K, V](kv: KV, k: K, v: V)
//
//  def encodeTransaction[CF[A, B] <: DbColumn[A, B], K, V](
//    txn: TransactionAction[CF[K, V]]
//  )(implicit codec: DbColumnCodec[K, V]): DbTransactionAction = {
//    txn match {
//      case TransactionPut(column, key, value) =>
//        DbTransactionAction(
//          DbTransactionAction.Action.Put(
//            DbPutRequest(
//              columnId = column.id,
//              key = ByteString.copyFrom(codec.encodeKey(key.asInstanceOf[K])),
//              value = ByteString.copyFrom(codec.encodeValue(value.asInstanceOf[V]))
//            )
//          )
//        )
//
//      case TransactionDelete(column, key) =>
//        DbTransactionAction(
//          DbTransactionAction.Action.Delete(
//            DbDeleteRequest(
//              columnId = column.id,
//              key = ByteString.copyFrom(codec.encodeKey(key.asInstanceOf[K]))
//            )
//          )
//        )
//
//      case TransactionDeletePrefix(column, prefix) =>
//        DbTransactionAction(
//          DbTransactionAction.Action.DeletePrefix(
//            DbDeletePrefixRequest(
//              columnId = column.id,
//              prefix = ByteString.copyFrom(prefix, StandardCharsets.UTF_8)
//            )
//          )
//        )
//    }
//  }

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

  def column[Col[A, B] <: DbDef#BaseCol[A, B], K, V](
    col: DbDef#Columns => Col[K, V]
  ): DbColumnClient[DbDef, Col, K, V] = {
    new DbColumnClient[DbDef, Col, K, V](db, col(db.definition.columns))
  }

  def column[Col[A, B] <: DbDef#BaseCol[A, B], K, V](
    col: Col[K, V]
  ): DbColumnClient[DbDef, Col, K, V] = {
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

  def closeTask(): RIO[Clock, Unit] = db.closeTask()

  def compactTask(): Task[Unit] = db.compactTask()

  def transactionTask(actions: Seq[DbTransactionAction]): Task[Seq[DbTransactionAction]] = {
    db.transactionTask(actions)
      .map(_ => actions)
  }

//  def transactionFlow(parallelism: Int = 1): Flow[Seq[DbTransactionAction], Seq[DbTransactionAction], NotUsed] = {
//    Flow[Seq[DbTransactionAction]]
//      .mapAsync(parallelism) { b =>
//        Future((b, b.map(a => encodeTransaction(a))))
//      }
//      .mapAsync(parallelism) {
//        case (b, encoded) =>
//          unsafeRunToFuture(
//            db.transactionTask(encoded)
//              .map(_ => b)
//          )
//      }
//  }

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
