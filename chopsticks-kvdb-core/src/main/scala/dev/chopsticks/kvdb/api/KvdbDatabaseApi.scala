package dev.chopsticks.kvdb.api

import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.kvdb.ColumnFamilyTransactionBuilder.TransactionAction
import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.kvdb.{ColumnFamily, ColumnFamilyTransactionBuilder, KvdbDatabase}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import squants.information.Information
import squants.information.InformationConversions._
import zio.{Task, ZIO}

import scala.concurrent.duration._

object KvdbDatabaseApi {
  final case class KvdbApiClientOptions(
    forceSync: Boolean = false,
    batchWriteMaxBatchSize: PosInt = 4096,
    batchWriteBatchingGroupWithin: FiniteDuration = Duration.Zero,
    batchReadMaxBatchBytes: Information = 32.kb,
    tailPollingMaxInterval: FiniteDuration = 100.millis,
    serdesParallelism: PosInt = 2
  )

  object KvdbApiClientOptions {
    val default: KvdbApiClientOptions = KvdbApiClientOptions()
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _],
    options: KvdbApiClientOptions
  ): ZIO[AkkaEnv, Nothing, KvdbDatabaseApi[BCF]] = ZIO.runtime[AkkaEnv].map { implicit rt =>
    val dbWithOptions: KvdbDatabase[BCF, _] = db.withOptions(
      _.copy(
        forceSync = options.forceSync,
        batchReadMaxBatchBytes = options.batchReadMaxBatchBytes,
        tailPollingMaxInterval = options.tailPollingMaxInterval
      )
    )

    new KvdbDatabaseApi[BCF](dbWithOptions, options)
  }
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (
  val db: KvdbDatabase[BCF, _],
  options: KvdbApiClientOptions
)(
  implicit rt: zio.Runtime[AkkaEnv]
) {
  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbDatabaseApi[BCF] = {
    val newOptions = modifier(options)

    new KvdbDatabaseApi[BCF](
      db.withOptions(
        _.copy(
          forceSync = options.forceSync,
          batchReadMaxBatchBytes = options.batchReadMaxBatchBytes,
          tailPollingMaxInterval = options.tailPollingMaxInterval
        )
      ),
      newOptions
    )
  }

  def columnFamily[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    col: CF[K, V] with CF2
  ): KvdbColumnFamilyApi[BCF, CF2, K, V] = {
    new KvdbColumnFamilyApi[BCF, CF2, K, V](db, col, options)
  }

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  def transactionTask(actions: Seq[TransactionAction]): Task[Seq[TransactionAction]] = {
    db.transactionTask(actions)
      .as(actions)
  }

  def transactionBuilder: ColumnFamilyTransactionBuilder[BCF] = db.transactionBuilder()
}
