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
import zio.{RIO, Task, URIO, ZIO}

import scala.concurrent.duration._
import io.scalaland.chimney.dsl._
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.util.KvdbSerdesThreadPool

import scala.concurrent.Future

object KvdbDatabaseApi {
  final case class KvdbApiClientOptions(
    forceSync: Boolean = false,
    batchWriteParallelism: PosInt = 1,
    batchWriteMaxBatchSize: PosInt = 4096,
    batchWriteBatchingGroupWithin: FiniteDuration = Duration.Zero,
    batchReadMaxBatchBytes: Information = 32.kb,
    tailPollingMaxInterval: FiniteDuration = 100.millis,
    tailPollingBackoffFactor: Double Refined Greater[W.`1.0d`.T] = 1.15d,
    disableIsolationGuarantee: Boolean = false,
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
        disableIsolationGuarantee = disableIsolationGuarantee,
        disableWriteConflictChecking = disableWriteConflictChecking,
        watchTimeout = watchTimeout,
        watchMinLatency = watchMinLatency
      )
    }

    def withDisabledIsolationGuarantee: KvdbApiClientOptions = copy(disableIsolationGuarantee = true)

    def withDisabledWriteConflictChecking: KvdbApiClientOptions = copy(disableWriteConflictChecking = true)
  }

  //noinspection TypeAnnotation
  object KvdbApiClientOptions {
    implicit val dbToApiOptionsPatcher = Patcher.derive[KvdbApiClientOptions, KvdbClientOptions]

    val default: KvdbApiClientOptions = KvdbApiClientOptions()
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _]
  ): URIO[AkkaEnv with MeasuredLogging with KvdbSerdesThreadPool, KvdbDatabaseApi[BCF]] =
    ZIO.runtime[AkkaEnv with MeasuredLogging with KvdbSerdesThreadPool].map { implicit rt =>
      new KvdbDatabaseApi[BCF](db, KvdbApiClientOptions.default.patchUsing(db.clientOptions))
    }
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (
  val db: KvdbDatabase[BCF, _],
  val options: KvdbApiClientOptions
)(implicit
  rt: zio.Runtime[AkkaEnv with MeasuredLogging with KvdbSerdesThreadPool]
) {
  private val serdesThreadPool = rt.environment.get[KvdbSerdesThreadPool.Service]

  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbDatabaseApi[BCF] = {
    val newOptions = modifier(options)

    new KvdbDatabaseApi[BCF](
      db.withOptions(newOptions.patchClientOptions),
      newOptions
    )
  }

  def batchTransact[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): Flow[A, P, NotUsed] = {
    Flow[A]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(batch)
        }(serdesThreadPool.executionContext)
      }
      .mapAsync(options.batchWriteParallelism) {
        case (writes, passthrough) =>
          db.transactionTask(writes)
            .as(passthrough)
            .unsafeRunToFuture
      }
  }

  def batchTransactUnordered[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): Flow[A, P, NotUsed] = {
    Flow[A]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .mapAsyncUnordered(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(batch)
        }(serdesThreadPool.executionContext)
      }
      .mapAsyncUnordered(options.batchWriteParallelism) {
        case (writes, passthrough) =>
          db.transactionTask(writes)
            .as(passthrough)
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
  def transactionTask(actions: Seq[TransactionWrite]): RIO[MeasuredLogging, Seq[TransactionWrite]] = {
    transact(actions)
  }

  def transact(actions: Seq[TransactionWrite]): RIO[MeasuredLogging, Seq[TransactionWrite]] = {
    db.transactionTask(actions)
      .as(actions)
  }

  def conditionallyTransact(
    reads: List[TransactionGet],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): RIO[MeasuredLogging, Seq[TransactionWrite]] = {
    db.conditionalTransactionTask(reads, condition, actions)
      .as(actions)
  }

  def readTransactionBuilder: KvdbReadTransactionBuilder[BCF] = db.readTransactionBuilder()
  def transactionBuilder: KvdbWriteTransactionBuilder[BCF] = db.transactionBuilder()
}
