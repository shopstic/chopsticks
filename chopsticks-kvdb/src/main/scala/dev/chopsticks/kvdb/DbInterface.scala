package dev.chopsticks.kvdb

import akka.NotUsed
import akka.stream.scaladsl.Source
import dev.chopsticks.kvdb.DbInterface.DbDefinition
import dev.chopsticks.kvdb.codec.DbKeyDecoder.DbKeyDecodeResult
import dev.chopsticks.kvdb.codec.DbValueDecoder.DbValueDecodeResult
import dev.chopsticks.kvdb.codec.{DbKey, DbValue}
import dev.chopsticks.kvdb.util.DbUtils._
import dev.chopsticks.kvdb.util.RocksdbCFBuilder.RocksdbCFOptions
import dev.chopsticks.kvdb.proto.DbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import enumeratum.EnumEntry
import enumeratum.EnumEntry.Snakecase
import zio.clock.Clock
import zio.{Task, RIO}

import scala.concurrent.Future
import scala.language.higherKinds

object DbInterface {

  abstract class DbColumn[K: DbKey, V: DbValue] extends EnumEntry with Snakecase {
    lazy val id: String = this.getClass.getName

    def dbKey: DbKey[K] = implicitly[DbKey[K]]
    def dbValue: DbValue[V] = implicitly[DbValue[V]]

    def rocksdbOptions: RocksdbCFOptions

    def encodeKey(key: K): Array[Byte] = dbKey.encode(key)
    def decodeKey(bytes: Array[Byte]): DbKeyDecodeResult[K] = dbKey.decode(bytes)

    def encodeValue(value: V): Array[Byte] = dbValue.encode(value)
    def decodeValue(bytes: Array[Byte]): DbValueDecodeResult[V] = dbValue.decode(bytes)

    def unsafeDecodeKey(bytes: Array[Byte]): K = {
      decodeKey(bytes) match {
        case Right(v) => v
        case Left(e) => throw e
      }
    }

    def unsafeDecodeValue(bytes: Array[Byte]): V = {
      decodeValue(bytes) match {
        case Right(v) => v
        case Left(e) => throw e
      }
    }

    def unsafeDecode(pair: DbPair): (K, V) = {
      (unsafeDecodeKey(pair._1), unsafeDecodeValue(pair._2))
    }
  }

  trait DbColumns[Col <: DbColumn[_, _]] extends enumeratum.Enum[Col]

  trait DbDefinition {
    type BaseCol[K, V] <: DbColumn[K, V]
    type Columns <: DbColumns[BaseCol[_, _]]
    def columns: Columns
  }

  class DbDefinitionOf[Col[K, V] <: DbColumn[K, V], E <: DbColumns[Col[_, _]]](val e: E) extends DbDefinition {
    type BaseCol[K, V] = Col[K, V]
    type Columns = e.type
    def columns: Columns = e
  }

  def keySatisfies(key: Array[Byte], constraints: List[DbKeyConstraint]): Boolean = {
    constraints.forall { c =>
      val operand = c.operand.toByteArray
      c.operator match {
        case Operator.FIRST | Operator.LAST => true
        case Operator.EQUAL => DbKey.isEqual(key, operand)
        case Operator.LESS_EQUAL => DbKey.compare(key, operand) <= 0
        case Operator.LESS => DbKey.compare(key, operand) < 0
        case Operator.GREATER => DbKey.compare(key, operand) > 0
        case Operator.GREATER_EQUAL => DbKey.compare(key, operand) >= 0
        case Operator.PREFIX => DbKey.isPrefix(operand, key)
        case Operator.Unrecognized(v) =>
          throw new IllegalArgumentException(s"Got Operator.Unrecognized($v)")
      }
    }
  }
}

trait DbInterface[DbDef <: DbDefinition] {
  def isLocal: Boolean

  def transactionBuilder(): DbColumnTransactionBuilder[DbDef] = {
    new DbColumnTransactionBuilder[DbDef](definition)
  }

  def statsTask: Task[Map[String, Double]]

  def definition: DbDef

  private lazy val columnFamilyByIdMap: Map[String, DbDef#BaseCol[_, _]] =
    definition.columns.values.map(c => (c.id, c)).toMap

  def columnFamilyWithId(id: String): Option[DbDef#BaseCol[_, _]] = columnFamilyByIdMap.get(id)

  def openTask(): Task[Unit]

  def getTask[Col <: DbDef#BaseCol[_, _]](column: Col, constraints: DbKeyConstraintList): Task[Option[DbPair]]

  def batchGetTask[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    requests: Seq[DbKeyConstraintList]
  ): Task[Seq[Option[DbPair]]]

  def estimateCount[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Long]

  def iterateSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbBatch, Future[NotUsed]]

  def iterateValuesSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbValueBatch, Future[NotUsed]]

  def putTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit]

  def deleteTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte]): Task[Unit]

  def deletePrefixTask[Col <: DbDef#BaseCol[_, _]](column: Col, prefix: Array[Byte]): Task[Long]

  def transactionTask(actions: Seq[DbTransactionAction]): Task[Unit]

  def tailSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbTailBatch, Future[NotUsed]]

  def tailValuesSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbTailValueBatch, Future[NotUsed]]

  def batchTailSource[Col <: DbDef#BaseCol[_, _]](column: Col, ranges: List[DbKeyRange])(
    implicit clientOptions: DbClientOptions
  ): Source[DbIndexedTailBatch, Future[NotUsed]]

  def dropColumnFamily[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Unit]

  def closeTask(): RIO[Clock, Unit]

  def compactTask(): Task[Unit]

  def startBulkInsertsTask(): Task[Unit]

  def endBulkInsertsTask(): Task[Unit]
}
