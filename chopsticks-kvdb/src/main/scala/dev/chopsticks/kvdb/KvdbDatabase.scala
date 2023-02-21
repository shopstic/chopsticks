package dev.chopsticks.kvdb

import dev.chopsticks.schema.config.SchemaConfig
import zio.schema.Schema

import java.time.Duration
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.*
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.*
import dev.chopsticks.kvdb.util.KvdbAliases.*
import dev.chopsticks.schema.SchemaAnnotation
import squants.information.Information
import squants.information.InformationConversions.*
import zio.{Schedule, Task}
import zio.stream.*

object KvdbDatabase:
  def keySatisfies(key: Array[Byte], constraints: List[KvdbKeyConstraint]): Boolean = {
    constraints.forall { c =>
      val operator = c.operator

      // Micro optimization to avoid "operand.toByteArray" for first and last operator
      if ((operator == Operator.FIRST || operator == Operator.LAST) && c.operand.isEmpty) {
        true
      }
      else {
        val operand = c.operand.toByteArray
        operator match {
          case Operator.EQUAL => KeySerdes.isEqual(key, operand)
          case Operator.LESS_EQUAL => KeySerdes.compare(key, operand) <= 0
          case Operator.LESS => KeySerdes.compare(key, operand) < 0
          case Operator.GREATER => KeySerdes.compare(key, operand) > 0
          case Operator.GREATER_EQUAL => KeySerdes.compare(key, operand) >= 0
          case Operator.PREFIX => KeySerdes.isPrefix(operand, key)
          case Operator.FIRST | Operator.LAST =>
            if (operand.isEmpty) true
            else {
              KeySerdes.isPrefix(operand, key)
            }
          case Operator.Unrecognized(v) =>
            throw new IllegalArgumentException(s"Got Operator.Unrecognized($v)")
        }
      }
    }
  }

  enum KvdbCustomWriteRetrySchedule:
    def getOrElse(fallback: => Schedule[Any, Throwable, Any]): Schedule[Any, Throwable, Any] =
      this match
        case KvdbCustomWriteRetrySchedule.None => fallback
        case KvdbCustomWriteRetrySchedule.Some(s) => s

    case None
    case Some(schedule: Schedule[Any, Throwable, Any])
  object KvdbCustomWriteRetrySchedule extends SchemaConfig[KvdbCustomWriteRetrySchedule]:
    import dev.chopsticks.schema.Schemas.*
    implicit override lazy val zioSchema: Schema[KvdbCustomWriteRetrySchedule] =
      summon[Schema[String]].mapBoth(_ => KvdbCustomWriteRetrySchedule.None, _ => "")

  final case class KvdbClientKnobs(
    @SchemaAnnotation.Default(Option.empty[Information])
    valueSizeLimit: Option[Information] = None
  )
  object KvdbClientKnobs extends SchemaConfig[KvdbClientKnobs]:
    import dev.chopsticks.schema.Schemas.*
    implicit override lazy val zioSchema: Schema[KvdbClientKnobs] = zio.schema.DeriveSchema.gen
    val default = KvdbClientKnobs()

  final case class KvdbClientOptions(
    @SchemaAnnotation.Default[Boolean](false)
    forceSync: Boolean = false,
    @SchemaAnnotation.Default[Information](32.kb)
    batchReadMaxBatchBytes: Information = 32.kb,
    @SchemaAnnotation.Default[Duration](Duration.ofMillis(100))
    tailPollingMaxInterval: Duration = Duration.ofMillis(100),
    @SchemaAnnotation.Default[Double](1.15d)
    tailPollingBackoffFactor: Double = 1.15d, // todo add back the refined value
    // tailPollingBackoffFactor: Double Refined Greater[1.0] = 1.15d,
    @SchemaAnnotation.Default[Boolean](false)
    disableIsolationGuarantee: Boolean = false,
    @SchemaAnnotation.Default[Boolean](false)
    disableWriteConflictChecking: Boolean = false,
    @SchemaAnnotation.Default[Boolean](false)
    useSnapshotReads: Boolean = false,
    @SchemaAnnotation.Default[Duration](Duration.ofSeconds(Long.MaxValue))
    watchTimeout: Duration = Duration.ofSeconds(Long.MaxValue), // infinity basically
    @SchemaAnnotation.Default[Duration](Duration.ofMillis(50))
    watchMinLatency: Duration = Duration.ofMillis(50),
    @SchemaAnnotation.Default[KvdbCustomWriteRetrySchedule](KvdbCustomWriteRetrySchedule.None)
    writeCustomRetrySchedule: KvdbCustomWriteRetrySchedule = KvdbCustomWriteRetrySchedule.None,
    @SchemaAnnotation.Default[KvdbClientKnobs](KvdbClientKnobs.default)
    knobs: KvdbClientKnobs = KvdbClientKnobs.default
  )
  object KvdbClientOptions extends SchemaConfig[KvdbClientOptions]:
    import dev.chopsticks.schema.Schemas.*
    implicit override lazy val zioSchema: Schema[KvdbClientOptions] = zio.schema.DeriveSchema.gen

// object KvdbClientOptions {
//   import dev.chopsticks.util.config.PureconfigConverters._
//   // Configuration of retry schedule via HOCON is not yet supported
//   implicit val writeRetryScheduleConfigConvert: ConfigConvert[Schedule[Any, Throwable, Any]] =
//     ConfigConvert.viaStringOpt[Schedule[Any, Throwable, Any]](_ => None, _ => "")
//   //noinspection TypeAnnotation
//   implicit val configConvert = ConfigConvert[KvdbClientOptions]
// }
end KvdbDatabase

trait KvdbDatabase[CFS <: ColumnFamily[_, _]]:
  def clientOptions: KvdbClientOptions

  def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[CFS]

  def materialization: KvdbMaterialization[CFS]

  def transactionBuilder(): KvdbWriteTransactionBuilder[CFS] = new KvdbWriteTransactionBuilder[CFS]

  def transactionFactory(): KvdbOperationFactory[CFS] = new KvdbOperationFactory[CFS]

  def readTransactionBuilder(): KvdbReadTransactionBuilder[CFS] = new KvdbReadTransactionBuilder[CFS]

  def statsTask: Task[Map[(String, Map[String, String]), Double]]

  protected lazy val columnFamilyByIdMap: Map[String, ColumnFamily[_, _]] =
    materialization.columnFamilySet.value.map(c => (c.id, c)).toMap

  def columnFamilyWithId(id: String): Option[ColumnFamily[_, _]] = columnFamilyByIdMap.get(id)

  def getTask[Col <: ColumnFamily[_, _]](column: Col, constraints: KvdbKeyConstraintList)(using
    CFS <:< Col
  ): Task[Option[KvdbPair]]

  def getRangeTask[Col <: ColumnFamily[_, _]](column: Col, range: KvdbKeyRange, reverse: Boolean = false)(using
    CFS <:< Col
  ): Task[List[KvdbPair]]

  def batchGetTask[Col <: ColumnFamily[_, _]](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  )(using CFS <:< Col): Task[Seq[Option[KvdbPair]]]

  def batchGetRangeTask[Col <: ColumnFamily[_, _]](
    column: Col,
    ranges: Seq[KvdbKeyRange],
    reverse: Boolean = false
  )(using CFS <:< Col): Task[Seq[List[KvdbPair]]]

  def estimateCount[Col <: ColumnFamily[_, _]](column: Col)(using CFS <:< Col): Task[Long]

//  @deprecated
  final def iterateSource[Col <: ColumnFamily[_, _]](
    column: Col,
    range: KvdbKeyRange
  )(using CFS <:< Col): Stream[Throwable, KvdbBatch] =
    iterateStream[Col](column, range)

  def iterateStream[Col <: ColumnFamily[_, _]](column: Col, range: KvdbKeyRange)(using
    CFS <:< Col
  ): Stream[Throwable, KvdbBatch]

  def putTask[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte], value: Array[Byte])(using
    CFS <:< Col
  ): Task[Unit]

  def deleteTask[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte])(using CFS <:< Col): Task[Unit]

  def deletePrefixTask[Col <: ColumnFamily[_, _]](column: Col, prefix: Array[Byte])(using CFS <:< Col): Task[Long]

  def transactionTask(actions: Seq[TransactionWrite]): Task[Unit]

  def conditionalTransactionTask(
    reads: List[TransactionRead.Get],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit]

  def tailSource[Col <: ColumnFamily[_, _]](column: Col, range: KvdbKeyRange)(using
    CFS <:< Col
  ): Stream[Throwable, KvdbTailBatch]

  def concurrentTailSource[Col <: ColumnFamily[_, _]](
    column: Col,
    ranges: List[KvdbKeyRange]
  )(using CFS <:< Col): Stream[Throwable, KvdbIndexedTailBatch]

  def dropColumnFamily[Col <: ColumnFamily[_, _]](column: Col)(using CFS <:< Col): Task[Unit]
end KvdbDatabase
