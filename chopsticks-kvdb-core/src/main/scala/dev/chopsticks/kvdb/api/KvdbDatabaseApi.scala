package dev.chopsticks.kvdb.api

import java.util.concurrent.TimeUnit
import akka.NotUsed
import akka.stream.scaladsl.Flow
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.iz_logging.IzLogging
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
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
import dev.chopsticks.stream.{AkkaStreamUtils, ZAkkaFlow}
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import eu.timepit.refined.types.numeric.PosInt
import io.scalaland.chimney.Patcher
import squants.information.Information
import squants.information.InformationConversions._
import zio.{RIO, Schedule, Task, URIO, ZIO}

import scala.concurrent.duration._
import io.scalaland.chimney.dsl._
import dev.chopsticks.kvdb.util.KvdbSerdesThreadPool
import dev.chopsticks.stream.ZAkkaFlow.FlowToZAkkaFlow
import pureconfig.ConfigConvert
import zio.clock.Clock

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
    useSnapshotReads: Boolean = false,
    serdesParallelism: PosInt = 2,
    watchTimeout: Duration = Duration.Inf,
    watchMinLatency: FiniteDuration = Duration(50, TimeUnit.MILLISECONDS),
    writeCustomRetrySchedule: Option[Schedule[Any, Throwable, Any]] = None
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

  //noinspection TypeAnnotation
  object KvdbApiClientOptions {
    implicit val dbToApiOptionsPatcher = Patcher.derive[KvdbApiClientOptions, KvdbClientOptions]

    val default: KvdbApiClientOptions = KvdbApiClientOptions()

    import dev.chopsticks.util.config.PureconfigConverters._
    // Configuration of retry schedule via HOCON is not yet supported
    implicit val writeRetryScheduleConfigConvert: ConfigConvert[Schedule[Any, Throwable, Any]] =
      ConfigConvert.viaStringOpt[Schedule[Any, Throwable, Any]](_ => None, _ => "")
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[KvdbApiClientOptions]
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B]](
    db: KvdbDatabase[BCF, _]
  ): URIO[AkkaEnv with IzLogging with Clock with KvdbSerdesThreadPool, KvdbDatabaseApi[BCF]] =
    ZIO.runtime[AkkaEnv with IzLogging with Clock with KvdbSerdesThreadPool].map { implicit rt =>
      new KvdbDatabaseApi[BCF](db, KvdbApiClientOptions.default.patchUsing(db.clientOptions))
    }
}

final class KvdbDatabaseApi[BCF[A, B] <: ColumnFamily[A, B]] private (
  val db: KvdbDatabase[BCF, _],
  val options: KvdbApiClientOptions
)(implicit
  rt: zio.Runtime[AkkaEnv with IzLogging with Clock with KvdbSerdesThreadPool]
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

  def withBackend(modifier: KvdbDatabase[BCF, _] => KvdbDatabase[BCF, _]): KvdbDatabaseApi[BCF] = {
    new KvdbDatabaseApi[BCF](
      modifier(db),
      options
    )
  }

  def batchTransact[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, A, P, NotUsed] = {
    batchThenTransact(buildTransaction)
  }

  def batchThenTransact[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, A, P, NotUsed] = {
    Flow[A]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .toZAkkaFlow
      .viaZAkkaFlow(transactBatches(buildTransaction))
  }

  def transactBatches[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, Vector[A], P, NotUsed] = {
    Flow[Vector[A]]
      .mapAsync(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(batch)
        }(serdesThreadPool.executionContext)
      }
      .toZAkkaFlow
      .mapAsync(options.batchWriteParallelism) {
        case (writes, passthrough) =>
          db.transactionTask(writes)
            .as(passthrough)
      }
  }

  def batchTransactUnordered[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, A, P, NotUsed] = {
    batchThenTransactUnordered(buildTransaction)
  }

  def batchThenTransactUnordered[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, A, P, NotUsed] = {
    Flow[A]
      .via(AkkaStreamUtils.batchFlow(options.batchWriteMaxBatchSize, options.batchWriteBatchingGroupWithin))
      .toZAkkaFlow
      .viaZAkkaFlow(transactBatchesUnordered(buildTransaction))
  }

  def transactBatchesUnordered[A, P](
    buildTransaction: Vector[A] => (List[TransactionWrite], P)
  ): ZAkkaFlow[IzLogging with Clock with AkkaEnv, Nothing, Vector[A], P, NotUsed] = {
    Flow[Vector[A]]
      .mapAsyncUnordered(options.serdesParallelism) { batch =>
        Future {
          buildTransaction(batch)
        }(serdesThreadPool.executionContext)
      }
      .toZAkkaFlow
      .mapAsyncUnordered(options.batchWriteParallelism) {
        case (writes, passthrough) =>
          db.transactionTask(writes)
            .as(passthrough)
      }
  }

  def columnFamily[CF[A, B] <: ColumnFamily[A, B], CF2 <: BCF[K, V], K, V](
    col: CF[K, V] with CF2
  ): KvdbColumnFamilyApi[BCF, CF2, K, V] = {
    new KvdbColumnFamilyApi[BCF, CF2, K, V](db, col, options)
  }

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = db.statsTask

  @deprecated("Use transact(...) instead", since = "2.13")
  def transactionTask(actions: Seq[TransactionWrite]): RIO[IzLogging with Clock, Seq[TransactionWrite]] = {
    transact(actions)
  }

  def transact(actions: Seq[TransactionWrite]): RIO[IzLogging with Clock, Seq[TransactionWrite]] = {
    db.transactionTask(actions)
      .as(actions)
  }

  def conditionallyTransact(
    reads: List[TransactionGet],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): RIO[IzLogging with Clock, Seq[TransactionWrite]] = {
    db.conditionalTransactionTask(reads, condition, actions)
      .as(actions)
  }

  def readTransactionBuilder: KvdbReadTransactionBuilder[BCF] = db.readTransactionBuilder()
  def transactionBuilder: KvdbWriteTransactionBuilder[BCF] = db.transactionBuilder()
  def transactionFactory: KvdbOperationFactory[BCF] = db.transactionFactory()
}
