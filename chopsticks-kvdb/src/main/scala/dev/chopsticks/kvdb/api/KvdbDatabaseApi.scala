package dev.chopsticks.kvdb.api

import java.time.Duration
import dev.chopsticks.kvdb.KvdbDatabase.{KvdbClientOptions, KvdbCustomWriteRetrySchedule}
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionRead
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.api.KvdbDatabaseApi.KvdbApiClientOptions
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import dev.chopsticks.kvdb.{
  ColumnFamily,
  KvdbDatabase,
  KvdbOperationFactory,
  KvdbReadTransactionBuilder,
  KvdbWriteTransactionBuilder
}
import eu.timepit.refined.auto.*
import eu.timepit.refined.types.numeric.PosInt
import squants.information.Information
import squants.information.InformationConversions.*
import zio.{Chunk, Task, URIO, ZIO}
import dev.chopsticks.kvdb.util.KvdbSerdesThreadPool
import dev.chopsticks.schema.SchemaAnnotation
import dev.chopsticks.schema.config.SchemaConfig
import dev.chopsticks.stream.StreamUtils
import zio.schema.Schema
import zio.stream.ZPipeline

object KvdbDatabaseApi {
  final case class KvdbApiClientOptions(
    @SchemaAnnotation.Default(false)
    forceSync: Boolean = false,
    @SchemaAnnotation.Default[PosInt](PosInt.unsafeFrom(1))
    batchWriteParallelism: PosInt = PosInt.unsafeFrom(1),
    @SchemaAnnotation.Default[PosInt](PosInt.unsafeFrom(4096))
    batchWriteMaxBatchSize: PosInt = PosInt.unsafeFrom(4096),
    @SchemaAnnotation.Default[Duration](Duration.ZERO)
    batchWriteBatchingGroupWithin: Duration = Duration.ZERO,
    @SchemaAnnotation.Default[Information](32.kb)
    batchReadMaxBatchBytes: Information = 32.kb,
    @SchemaAnnotation.Default[Duration](Duration.ofMillis(100))
    tailPollingMaxInterval: Duration = Duration.ofMillis(100),
    @SchemaAnnotation.Default[Double](1.15d)
    tailPollingBackoffFactor: Double = 1.15d, // todo add refined value (> 1.0d)
    @SchemaAnnotation.Default(false)
    disableIsolationGuarantee: Boolean = false,
    @SchemaAnnotation.Default(false)
    disableWriteConflictChecking: Boolean = false,
    @SchemaAnnotation.Default[Boolean](false)
    useSnapshotReads: Boolean = false,
    @SchemaAnnotation.Default[PosInt](PosInt.unsafeFrom(2))
    serdesParallelism: PosInt = PosInt.unsafeFrom(2),
    @SchemaAnnotation.Default[Duration](Duration.ofSeconds(Long.MaxValue))
    watchTimeout: Duration = Duration.ofSeconds(Long.MaxValue), // infinity basically
    @SchemaAnnotation.Default[Duration](Duration.ofMillis(50))
    watchMinLatency: Duration = Duration.ofMillis(50),
    @SchemaAnnotation.Default[KvdbCustomWriteRetrySchedule](KvdbCustomWriteRetrySchedule.None)
    writeCustomRetrySchedule: KvdbCustomWriteRetrySchedule = KvdbCustomWriteRetrySchedule.None
  ) {
    def patchClientOptions(options: KvdbClientOptions): KvdbClientOptions = {
      options.copy(
        forceSync = forceSync,
        batchReadMaxBatchBytes = batchReadMaxBatchBytes,
        tailPollingMaxInterval = tailPollingMaxInterval,
        tailPollingBackoffFactor = tailPollingBackoffFactor,
        disableIsolationGuarantee = disableIsolationGuarantee,
        disableWriteConflictChecking = disableWriteConflictChecking,
        useSnapshotReads = useSnapshotReads,
        watchTimeout = watchTimeout,
        watchMinLatency = watchMinLatency,
        writeCustomRetrySchedule = writeCustomRetrySchedule
      )
    }

    def withDisabledIsolationGuarantee: KvdbApiClientOptions = copy(disableIsolationGuarantee = true)

    def withDisabledWriteConflictChecking: KvdbApiClientOptions = copy(disableWriteConflictChecking = true)

    def withSnapshotReads: KvdbApiClientOptions = copy(useSnapshotReads = true)
  }

  object KvdbApiClientOptions extends SchemaConfig[KvdbApiClientOptions]:
    import dev.chopsticks.schema.Schemas.*
    val default: KvdbApiClientOptions = KvdbApiClientOptions()
    implicit override lazy val zioSchema: Schema[KvdbApiClientOptions] = zio.schema.DeriveSchema.gen

  def apply[CFS <: ColumnFamily[_, _]](
    db: KvdbDatabase[CFS]
  ): URIO[KvdbSerdesThreadPool, KvdbDatabaseApi[CFS]] =
    ZIO.runtime[KvdbSerdesThreadPool].map { implicit rt =>
      new KvdbDatabaseApi[CFS](
        db,
        KvdbApiClientOptions(
          forceSync = db.clientOptions.forceSync,
          batchWriteParallelism = KvdbApiClientOptions.default.batchWriteParallelism,
          batchWriteMaxBatchSize = KvdbApiClientOptions.default.batchWriteMaxBatchSize,
          batchWriteBatchingGroupWithin = KvdbApiClientOptions.default.batchWriteBatchingGroupWithin,
          batchReadMaxBatchBytes = db.clientOptions.batchReadMaxBatchBytes,
          tailPollingMaxInterval = db.clientOptions.tailPollingMaxInterval,
          tailPollingBackoffFactor = db.clientOptions.tailPollingBackoffFactor,
          disableIsolationGuarantee = db.clientOptions.disableIsolationGuarantee,
          disableWriteConflictChecking = db.clientOptions.disableWriteConflictChecking,
          useSnapshotReads = db.clientOptions.useSnapshotReads,
          serdesParallelism = KvdbApiClientOptions.default.serdesParallelism,
          watchTimeout = db.clientOptions.watchTimeout,
          watchMinLatency = db.clientOptions.watchMinLatency,
          writeCustomRetrySchedule = db.clientOptions.writeCustomRetrySchedule
        )
      )
    }
}

final class KvdbDatabaseApi[CFS <: ColumnFamily[_, _]] private (
  val db: KvdbDatabase[CFS],
  val options: KvdbApiClientOptions
)(implicit
  rt: zio.Runtime[KvdbSerdesThreadPool]
):
  import dev.chopsticks.stream.StreamUtils.*

  private val serdesThreadPool = rt.environment.get[KvdbSerdesThreadPool]

  def withOptions(
    modifier: KvdbApiClientOptions => KvdbApiClientOptions
  ): KvdbDatabaseApi[CFS] = {
    val newOptions = modifier(options)

    new KvdbDatabaseApi[CFS](
      db.withOptions(newOptions.patchClientOptions),
      newOptions
    )
  }

  def withBackend(modifier: KvdbDatabase[CFS] => KvdbDatabase[CFS]): KvdbDatabaseApi[CFS] = {
    new KvdbDatabaseApi[CFS](
      modifier(db),
      options
    )
  }

  def batchTransact[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, A, P] =
    batchThenTransact(buildTransaction)

  def batchThenTransact[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, A, P] =
    StreamUtils.batchPipeline[A](options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin) >>>
      transactBatches(buildTransaction)

  def transactBatches[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, Chunk[A], P] =
    ZPipeline
      .mapZIOPar(options.serdesParallelism) { (batch: Chunk[A]) =>
        ZIO
          .attempt(buildTransaction(batch))
          .onExecutor(serdesThreadPool.executor)
      }
      .mapZIOPar(options.batchWriteParallelism) {
        case (writes: List[TransactionWrite] @unchecked, passthrough: P @unchecked) =>
          db.transactionTask(writes).as(passthrough)
      }

  def batchTransactUnordered[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, A, P] =
    batchThenTransactUnordered(buildTransaction)

  def batchThenTransactUnordered[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, A, P] =
    StreamUtils.batchPipeline[A](options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin) >>>
      transactBatchesUnordered(buildTransaction)

  def transactBatchesUnordered[A, P](
    buildTransaction: Chunk[A] => (List[TransactionWrite], P)
  ): ZPipeline[Any, Throwable, Chunk[A], P] =
    ZPipeline
      .mapZIOParUnordered(options.serdesParallelism) { (batch: Chunk[A]) =>
        ZIO
          .attempt(buildTransaction(batch))
          .onExecutor(serdesThreadPool.executor)
      }
      .mapZIOParUnordered(options.batchWriteParallelism) {
        case (writes: List[TransactionWrite] @unchecked, passthrough: P @unchecked) =>
          db.transactionTask(writes).as(passthrough)
      }

  def columnFamily[CF <: ColumnFamily[_, _]](col: CF)(using CFS <:< CF): KvdbColumnFamilyApi[CF, CFS] =
    new KvdbColumnFamilyApi[CF, CFS](db, col, options)

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  def transact(actions: Seq[TransactionWrite]): Task[Seq[TransactionWrite]] =
    db
      .transactionTask(actions)
      .as(actions)

  def conditionallyTransact(
    reads: List[TransactionRead.Get],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Seq[TransactionWrite]] =
    db
      .conditionalTransactionTask(reads, condition, actions)
      .as(actions)

  def readTransactionBuilder: KvdbReadTransactionBuilder[CFS] = db.readTransactionBuilder()
  def transactionBuilder: KvdbWriteTransactionBuilder[CFS] = db.transactionBuilder()
  def transactionFactory: KvdbOperationFactory[CFS] = db.transactionFactory()

end KvdbDatabaseApi
