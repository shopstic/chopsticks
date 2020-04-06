package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.kvdb.ColumnFamilyTransactionBuilder.TransactionAction
import dev.chopsticks.kvdb.{ColumnFamily, ColumnFamilyTransactionBuilder, KvdbDatabase}
import zio.{Task, ZIO}

object KvdbDatabaseApi {
  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _]
  ): ZIO[AkkaEnv, Nothing, KvdbDatabaseApi[BCF]] = ZIO.runtime[AkkaEnv].map(implicit rt => new KvdbDatabaseApi[BCF](db))
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (val db: KvdbDatabase[BCF, _])(
  implicit rt: zio.Runtime[AkkaEnv]
) {
  def columnFamily[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    col: CF[K, V] with CF2
  ): KvdbColumnFamilyApi[BCF, CF2, K, V] = {
    new KvdbColumnFamilyApi[BCF, CF2, K, V](db, col)
  }

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  def transactionTask(actions: Seq[TransactionAction], sync: Boolean = false): Task[Seq[TransactionAction]] = {
    db.transactionTask(actions, sync)
      .as(actions)
  }

  def transactionBuilder: ColumnFamilyTransactionBuilder[BCF] = db.transactionBuilder()
}
