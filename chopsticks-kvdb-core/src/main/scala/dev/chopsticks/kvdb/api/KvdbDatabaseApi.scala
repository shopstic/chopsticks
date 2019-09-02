package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.proto.KvdbTransactionAction
import dev.chopsticks.kvdb.{ColumnFamily, ColumnFamilyTransactionBuilder, KvdbDatabase}
import zio.clock.Clock
import zio.{RIO, Task, ZIO}

import scala.language.higherKinds

object KvdbDatabaseApi {
  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _]
  ): ZIO[AkkaEnv, Nothing, KvdbDatabaseApi[BCF]] = ZIO.access[AkkaEnv](implicit env => new KvdbDatabaseApi[BCF](db))
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (val db: KvdbDatabase[BCF, _])(
  implicit akkaEnv: AkkaEnv
) {
  def columnFamily[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    col: CF[K, V] with CF2
  ): KvdbColumnFamilyApi[BCF, CF2, K, V] = {
    new KvdbColumnFamilyApi[BCF, CF2, K, V](db, col)
  }

  def openTask(): Task[this.type] = {
    db.openTask()
      .map(_ => this)
  }

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  def closeTask(): RIO[Clock, Unit] = db.closeTask()

  def transactionTask(actions: Seq[KvdbTransactionAction]): Task[Seq[KvdbTransactionAction]] = {
    db.transactionTask(actions)
      .map(_ => actions)
  }

  def transactionBuilder: ColumnFamilyTransactionBuilder[BCF] = db.transactionBuilder()
}
