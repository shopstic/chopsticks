package dev.chopsticks.kvdb

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Source
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.KvdbDatabase.KvdbClientOptions
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.kvdb.util.KvdbAliases._
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import pureconfig.ConfigConvert
import squants.information.Information
import squants.information.InformationConversions._
import zio.Task

import scala.concurrent.duration._

object KvdbDatabase {
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

  final case class KvdbClientOptions(
    forceSync: Boolean = false,
    batchReadMaxBatchBytes: Information = 32.kb,
    tailPollingMaxInterval: FiniteDuration = 100.millis,
    tailPollingBackoffFactor: Double Refined Greater[W.`1.0d`.T] = 1.15d,
    disableWriteConflictChecking: Boolean = false,
    watchTimeout: Duration = Duration.Inf,
    watchMinLatency: FiniteDuration = Duration(50, TimeUnit.MILLISECONDS)
  )

  object KvdbClientOptions {
    import dev.chopsticks.util.config.PureconfigConverters._
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[KvdbClientOptions]
  }
}

trait KvdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] {
  type CF = BCF[_, _]

  def clientOptions: KvdbClientOptions

  def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[BCF, CFS]

  def materialization: KvdbMaterialization[BCF, CFS]

  def transactionBuilder(): KvdbWriteTransactionBuilder[BCF] = new KvdbWriteTransactionBuilder[BCF]

  def readTransactionBuilder(): KvdbReadTransactionBuilder[BCF] = new KvdbReadTransactionBuilder[BCF]

  def statsTask: Task[Map[(String, Map[String, String]), Double]]

  protected lazy val columnFamilyByIdMap: Map[String, CF] =
    materialization.columnFamilySet.value.map(c => (c.id, c)).toMap

  def columnFamilyWithId(id: String): Option[CF] = columnFamilyByIdMap.get(id)

  def getTask[Col <: CF](column: Col, constraints: KvdbKeyConstraintList): Task[Option[KvdbPair]]

  def getRangeTask[Col <: CF](column: Col, range: KvdbKeyRange): Task[List[KvdbPair]]

  def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]]

  def batchGetRangeTask[Col <: CF](
    column: Col,
    ranges: Seq[KvdbKeyRange]
  ): Task[Seq[List[KvdbPair]]]

  def estimateCount[Col <: CF](column: Col): Task[Long]

  def watchKeySource[Col <: CF](column: Col, key: Array[Byte]): Source[Option[Array[Byte]], NotUsed]

  def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbBatch, NotUsed]

  def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit]

  def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit]

  def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long]

  def transactionTask(actions: Seq[TransactionWrite]): Task[Unit]

  def conditionalTransactionTask(
    reads: List[TransactionGet],
    condition: List[Option[KvdbPair]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit]

  def tailSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbTailBatch, NotUsed]

  def concurrentTailSource[Col <: CF](column: Col, ranges: List[KvdbKeyRange]): Source[KvdbIndexedTailBatch, NotUsed]

  def dropColumnFamily[Col <: CF](column: Col): Task[Unit]
}
