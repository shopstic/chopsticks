package dev.chopsticks.kvdb.api

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbReadTransactionBuilder, KvdbWriteTransactionBuilder}
import dev.chopsticks.stream.AkkaStreamUtils
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import eu.timepit.refined.types.numeric.PosInt
import io.scalaland.chimney.Patcher
import squants.information.Information
import squants.information.InformationConversions._
import zio.{Task, ZIO}

import scala.concurrent.duration._
import io.scalaland.chimney.dsl._
import dev.chopsticks.fp.zio_ext._

import scala.concurrent.Future

object KvdbDatabaseApi {
  final case class KvdbApiClientOptions(
    forceSync: Boolean = false,
    batchWriteMaxBatchSize: PosInt = 4096,
    batchWriteBatchingGroupWithin: FiniteDuration = Duration.Zero,
    batchReadMaxBatchBytes: Information = 32.kb,
    tailPollingMaxInterval: FiniteDuration = 100.millis,
    tailPollingBackoffFactor: Double Refined Greater[W.`1.0d`.T] = 1.15d,
    disableWriteConflictChecking: Boolean = false,
    serdesParallelism: PosInt = 2,
    watchTimeout: Duration = Duration.Inf,
    watchMinLatency: FiniteDuration = Duration(50, TimeUnit.MILLISECONDS)
  ) {
    def patchClientOptions(options: KvdbClientOptions): KvdbClientOptions = {
      options.copy(
        forceSync = forceSync,
        batchReadMaxBatchBytes = batchReadMaxBatchBytes,
        tailPollingMaxInterval = tailPollingMaxInterval,
        tailPollingBackoffFactor = tailPollingBackoffFactor,
        disableWriteConflictChecking = disableWriteConflictChecking,
        watchTimeout = watchTimeout,
        watchMinLatency = watchMinLatency
      )
    }
  }

  //noinspection TypeAnnotation
  object KvdbApiClientOptions {
    implicit val dbToApiOptionsPatcher = Patcher.derive[KvdbApiClientOptions, KvdbClientOptions]

    val default: KvdbApiClientOptions = KvdbApiClientOptions()
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _]
  ): ZIO[AkkaEnv, Nothing, KvdbDatabaseApi[BCF]] = ZIO.runtime[AkkaEnv].map { implicit rt =>
    new KvdbDatabaseApi[BCF](db, KvdbApiClientOptions.default.patchUsing(db.clientOptions))
  }
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (
  val db: KvdbDatabase[BCF, _],
  options: KvdbApiClientOptions
)(implicit
  rt: zio.Runtime[AkkaEnv]
) {
  private val akkaEnv = rt.environment.get[AkkaEnv.Service]
  import akkaEnv._

  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbDatabaseApi[BCF] = {
    val newOptions = modifier(options)

    new KvdbDatabaseApi[BCF](
      db.withOptions(options.patchClientOptions),
      newOptions
    )
  }

  def batchTransact[A](
    buildTransaction: (KvdbWriteTransactionBuilder[BCF], Vector[A]) => KvdbWriteTransactionBuilder[BCF]
  ): Flow[A, Seq[A], NotUsed] = {
    Flow[A]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(db.transactionBuilder(), batch)
            .result
        }.map(serialized => (batch, serialized))
      }
      .mapAsync(1) {
        case (batch, serialized) =>
          db.transactionTask(serialized)
            .as(batch)
            .unsafeRunToFuture
      }
  }

  def columnFamily[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    col: CF[K, V] with CF2
  ): KvdbColumnFamilyApi[BCF, CF2, K, V] = {
    new KvdbColumnFamilyApi[BCF, CF2, K, V](db, col, options)
  }

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  @deprecated("Use transact(...) instead", since = "2.13")
  def transactionTask(actions: Seq[TransactionWrite]): Task[Seq[TransactionWrite]] = {
    transact(actions)
  }

  def transact(actions: Seq[TransactionWrite]): Task[Seq[TransactionWrite]] = {
    db.transactionTask(actions)
      .as(actions)
  }

  def conditionallyTransact(
    reads: List[TransactionGet],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Seq[TransactionWrite]] = {
    db.conditionalTransactionTask(reads, condition, actions)
      .as(actions)
  }

  def readTransactionBuilder: KvdbReadTransactionBuilder[BCF] = db.readTransactionBuilder()
  def transactionBuilder: KvdbWriteTransactionBuilder[BCF] = db.transactionBuilder()
}
