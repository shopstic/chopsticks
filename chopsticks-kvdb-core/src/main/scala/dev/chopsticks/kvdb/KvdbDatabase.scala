package dev.chopsticks.kvdb

import akka.NotUsed
import akka.stream.scaladsl.Source
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbClientOptions
import dev.chopsticks.kvdb.util.KvdbException.KvdbAlreadyClosedException
import zio.clock.Clock
import zio.{RIO, Task, ZIO, ZManaged}

import scala.concurrent.Future
import scala.language.higherKinds

object KvdbDatabase {

  def keySatisfies(key: Array[Byte], constraints: List[KvdbKeyConstraint]): Boolean = {
    constraints.forall { c =>
      val operator = c.operator

      if (operator == Operator.FIRST || operator == Operator.LAST) {
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
          case Operator.FIRST | Operator.LAST => true
          case Operator.Unrecognized(v) =>
            throw new IllegalArgumentException(s"Got Operator.Unrecognized($v)")
        }
      }
    }
  }

  def manage[R, BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    db: ZIO[R, Throwable, KvdbDatabase[BCF, CFS]]
  ): ZManaged[R with Clock, Throwable, KvdbDatabase[BCF, CFS]] = {
    ZManaged.make[R with Clock, Throwable, KvdbDatabase[BCF, CFS]](db) {
      _.closeTask().catchAll {
        case _: KvdbAlreadyClosedException => ZIO.unit
        case t => ZIO.die(t)
      }
    }
  }
}

trait KvdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] {

  type CF = BCF[_, _]

  def isLocal: Boolean

  def materialization: KvdbMaterialization[BCF, CFS]

  def transactionBuilder(): ColumnFamilyTransactionBuilder[BCF] = new ColumnFamilyTransactionBuilder[BCF]

  def statsTask: Task[Map[(String, Map[String, String]), Double]]

  private lazy val columnFamilyByIdMap: Map[String, CF] =
    materialization.columnFamilySet.value.map(c => (c.id, c)).toMap

  def columnFamilyWithId(id: String): Option[CF] = columnFamilyByIdMap.get(id)

  def openTask(): Task[Unit]

  def getTask[Col <: CF](column: Col, constraints: KvdbKeyConstraintList): Task[Option[KvdbPair]]

  def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]]

  def estimateCount[Col <: CF](column: Col): Task[Long]

  def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbBatch, Future[NotUsed]]

  def iterateValuesSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbValueBatch, Future[NotUsed]]

  def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit]

  def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit]

  def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long]

  def transactionTask(actions: Seq[KvdbTransactionAction]): Task[Unit]

  def tailSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbTailBatch, Future[NotUsed]]

  def tailValuesSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbTailValueBatch, Future[NotUsed]]

  def batchTailSource[Col <: CF](column: Col, ranges: List[KvdbKeyRange])(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbIndexedTailBatch, Future[NotUsed]]

  def dropColumnFamily[Col <: CF](column: Col): Task[Unit]

  def closeTask(): RIO[Clock, Unit]
}
