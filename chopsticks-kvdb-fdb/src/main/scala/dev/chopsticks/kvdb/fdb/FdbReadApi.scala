package dev.chopsticks.kvdb.fdb

import cats.syntax.either._
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.apple.foundationdb.{KeySelector, ReadTransaction}
import dev.chopsticks.kvdb.ColumnFamily
import dev.chopsticks.kvdb.KvdbDatabase.keySatisfies
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.fdb.FdbDatabase.FdbContext
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases.KvdbPair
import dev.chopsticks.kvdb.util.KvdbException.InvalidKvdbArgumentException
import eu.timepit.refined.types.numeric.PosInt

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

class FdbReadApi[BCF[A, B] <: ColumnFamily[A, B]](
  val tx: ReadTransaction,
  dbContext: FdbContext[BCF]
) {
  type CF = BCF[_, _]

  def snapshot(): FdbReadApi[BCF] = {
    new FdbReadApi[BCF](tx.snapshot(), dbContext)
  }

  def nonEqualFromConstraintToKeySelector(operator: Operator, operand: Array[Byte]): KeySelector = {
    operator match {
      case Operator.PREFIX =>
        KeySelector.firstGreaterOrEqual(operand)
      case Operator.GREATER =>
        KeySelector.firstGreaterThan(operand)
      case Operator.LESS =>
        KeySelector.lastLessThan(operand)
      case Operator.GREATER_EQUAL =>
        KeySelector.firstGreaterOrEqual(operand)
      case Operator.LESS_EQUAL =>
        KeySelector.lastLessOrEqual(operand)
      case Operator.FIRST =>
        KeySelector.firstGreaterOrEqual(operand)
      case Operator.LAST =>
        KeySelector.lastLessOrEqual(ByteArrayUtil.strinc(operand))
      case Operator.EQUAL =>
        throw new IllegalArgumentException("Constraint must not have EQUAL operator")
      case o =>
        throw new IllegalArgumentException(s"Unrecognized operator: $o")
    }
  }

  def toConstraintToKeySelector(
    operator: Operator,
    operand: Array[Byte],
    firstOutOfRangeOperand: Array[Byte]
  ): KeySelector = {
    operator match {
      case Operator.EQUAL =>
        KeySelector.firstGreaterOrEqual(operand)
      case Operator.LESS =>
        KeySelector.firstGreaterOrEqual(operand)
      case Operator.LESS_EQUAL =>
        KeySelector.firstGreaterThan(operand)
      case Operator.PREFIX =>
        KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(operand))
      case _ =>
        KeySelector.firstGreaterOrEqual(firstOutOfRangeOperand)
    }
  }

  private[chopsticks] def getEitherByColumnId(
    columnId: String,
    constraints: List[KvdbKeyConstraint]
  ): CompletableFuture[Either[Array[Byte], KvdbPair]] = {
    if (constraints.isEmpty) {
      CompletableFuture.completedFuture(Left(Array.emptyByteArray))
    }
    else {
      val prefixedConstraints = dbContext.prefixKeyConstraints(columnId, constraints)
      val headConstraint = prefixedConstraints.head
      val headOperand = headConstraint.operand.toByteArray
      val operator = headConstraint.operator

      operator match {
        case Operator.EQUAL =>
          tx.get(headOperand).thenApply { value =>
            if (value != null && keySatisfies(headOperand, prefixedConstraints.tail)) {
              Right((headOperand, value))
            }
            else Left(headOperand)
          }

        case op =>
          val keySelector = nonEqualFromConstraintToKeySelector(operator, headOperand)
          val keyFuture = tx.getKey(keySelector)
          val tailConstraints = if (op == Operator.PREFIX) prefixedConstraints else prefixedConstraints.tail

          val ret: CompletableFuture[Either[Array[Byte], KvdbPair]] = keyFuture.thenCompose { key: Array[Byte] =>
            if (
              key != null && key.length > 0 && KeySerdes.isPrefix(
                dbContext.columnPrefix(columnId),
                key
              ) && keySatisfies(
                key,
                tailConstraints
              )
            ) {
              tx.get(key).thenApply(value => Either.right[Array[Byte], KvdbPair]((key, value)))
            }
            else {
              CompletableFuture.completedFuture(Either.left[Array[Byte], KvdbPair](Array.emptyByteArray))
            }
          }

          ret
      }
    }
  }

  def addReadConflictKeyIfNotSnapshot[Col <: CF](column: Col, key: Array[Byte]): Boolean = {
    tx.addReadConflictKeyIfNotSnapshot(dbContext.prefixKey(column, key))
  }

  def addReadConflictRangeIfNotSnapshot[Col <: CF](column: Col, begin: Array[Byte], end: Array[Byte]): Boolean = {
    tx.addReadConflictRangeIfNotSnapshot(dbContext.prefixKey(column, begin), dbContext.prefixKey(column, end))
  }

  def getEither[Col <: CF](
    column: Col,
    constraints: List[KvdbKeyConstraint]
  ): CompletableFuture[Either[Array[Byte], KvdbPair]] = {
    getEitherByColumnId(column.id, constraints)
  }

  private def doGetRangeFuture[Col <: CF](
    column: Col,
    from: List[KvdbKeyConstraint],
    to: List[KvdbKeyConstraint],
    limit: PosInt
  ): CompletableFuture[List[KvdbPair]] = {
    val prefixedFrom = dbContext.prefixKeyConstraints(column, from)
    val prefixedTo = dbContext.prefixKeyConstraints(column, to)
    val fromHead = prefixedFrom.head
    val operator = fromHead.operator
    val headValidation =
      if (operator == Operator.EQUAL || operator == Operator.PREFIX) prefixedFrom else prefixedFrom.tail
    val headOperand = fromHead.operand.toByteArray

    val startKeySelector = operator match {
      case Operator.EQUAL => KeySelector.firstGreaterOrEqual(headOperand)
      case _ => nonEqualFromConstraintToKeySelector(operator, headOperand)
    }

    val toHead = prefixedTo.head
    val endKeySelector = toConstraintToKeySelector(
      toHead.operator,
      toHead.operand.toByteArray,
      dbContext.strinc(column)
    )
    val columnPrefix = dbContext.columnPrefix(column)

    tx.getRange(startKeySelector, endKeySelector, limit.value)
      .asList()
      .thenApply { javaList =>
        val pairs = javaList.asScala.toList match {
          case head :: tail =>
            val headKey = head.getKey
            if (
              headKey != null && headKey.nonEmpty && KeySerdes.isPrefix(columnPrefix, headKey) && keySatisfies(
                headKey,
                headValidation
              )
            ) {
              head :: tail.takeWhile { p =>
                val key = p.getKey
                key != null && key.nonEmpty && KeySerdes.isPrefix(columnPrefix, headKey) && keySatisfies(
                  key,
                  prefixedTo
                )
              }
            }
            else Nil
          case Nil => Nil
        }

        pairs.map(p => dbContext.unprefixKey(column, p.getKey) -> p.getValue)
      }
  }

  def getByColumnId(
    columnId: String,
    constraints: List[KvdbKeyConstraint]
  ): CompletableFuture[Option[KvdbPair]] = {
    getEitherByColumnId(columnId, constraints).thenApply { // noinspection MatchToPartialFunction
      result =>
        result match {
          case Right((key, value)) =>
            Some(dbContext.unprefixKey(columnId, key) -> value)
          case _ => None
        }
    }
  }

  def get[Col <: CF](
    column: Col,
    constraints: List[KvdbKeyConstraint]
  ): CompletableFuture[Option[KvdbPair]] = {
    getEither(column, constraints).thenApply { // noinspection MatchToPartialFunction
      result =>
        result match {
          case Right((key, value)) =>
            Some(dbContext.unprefixKey(column, key) -> value)
          case _ => None
        }
    }
  }

  def getRange[Col <: CF](
    column: Col,
    range: KvdbKeyRange
  ): CompletableFuture[List[KvdbPair]] = {
    PosInt.from(range.limit) match {
      case Left(_) =>
        CompletableFuture.failedFuture(
          InvalidKvdbArgumentException(s"range.limit of '${range.limit}' is invalid, must be a positive integer")
        )
      case Right(limit) =>
        doGetRangeFuture(column, range.from, range.to, limit)
    }
  }
}
