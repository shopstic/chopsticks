package dev.chopsticks.kvdb.fdb

import java.util
import java.util.concurrent.CompletableFuture

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.apple.foundationdb.{Database, FDB, KeySelector, ReadTransaction, Transaction}
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.LoggingContext
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.KvdbDatabase.keySatisfies
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.{KvdbClientOptions, KvdbCloseSignal}
import dev.chopsticks.kvdb.{ColumnFamily, ColumnFamilyTransactionBuilder, KvdbDatabase, KvdbMaterialization}
import zio.blocking.{blocking, Blocking}
import zio.{Task, UIO, ZIO, ZManaged}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import cats.syntax.either._
import cats.syntax.show._
import dev.chopsticks.kvdb.ColumnFamilyTransactionBuilder.{TransactionDelete, TransactionDeleteRange, TransactionPut}
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits._
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.util.KvdbException.SeekFailure

object FdbDatabase extends LoggingContext {
  final case class FdbContext[BCF[A, B] <: ColumnFamily[A, B]](db: Database, prefixMap: Map[BCF[_, _], Array[Byte]]) {
    val dbCloseSignal = new KvdbCloseSignal

    private val columnIdPrefixMap = prefixMap.map {
      case (k, v) =>
        k.id -> v
    }

    def columnPrefix[CF <: BCF[_, _]](column: CF): Array[Byte] = {
      prefixMap(column)
    }

    def strinc[CF <: BCF[_, _]](column: CF): Array[Byte] = {
      ByteArrayUtil.strinc(prefixMap(column))
    }

    def prefixKey(columnId: String, key: Array[Byte]): Array[Byte] = {
      val prefix = columnIdPrefixMap(columnId)
      ByteArrayUtil.join(prefix, key)
    }

    def prefixKey[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      val prefix = prefixMap(column)
      ByteArrayUtil.join(prefix, key)
    }

    def unprefixKey[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      val prefixLength = prefixMap(column).length
      util.Arrays.copyOfRange(key, prefixLength, key.length + 1)
    }

    def prefixKeyConstraints[CF <: BCF[_, _]](
      column: CF,
      constraints: List[KvdbKeyConstraint]
    ): List[KvdbKeyConstraint] = {
      val prefix = prefixMap(column)
      constraints.map { constraint =>
        constraint
          .copy(
            operand = ByteString.copyFrom(prefix).concat(constraint.operand),
            operandDisplay =
              s"[columnId=${column.id}][columnPrefix=${ByteArrayUtil.printable(prefix)}][operand=${constraint.operandDisplay}]"
          )
      }
    }
  }

  final case class Config(
    clusterFilePath: Option[String]
  )

  private def buildPrefixMap[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    db: Database,
    materialization: KvdbMaterialization[BCF, CFS]
  ): Task[Map[BCF[_, _], Array[Byte]]] = {
    Task.fromCompletionStage[Map[BCF[_, _], Array[Byte]]] {
      db.runAsync { tx =>
        val directory = DirectoryLayer.getDefault
        val futures = materialization.columnFamilySet.value.toList.map { cf =>
          directory
            .createOrOpen(tx, List(cf.id).asJava)
            .thenApply { dir => cf -> dir.pack() }
        }

        CompletableFuture
          .allOf(futures: _*)
          .thenApply(_ => futures.map(_.join()).toMap)
      }
    }
  }

  def manage[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS],
    config: Config
  ): ZManaged[AkkaEnv with Blocking with MeasuredLogging, Throwable, KvdbDatabase[BCF, CFS]] = {
    val managedContext = ZManaged.make {
      for {
        db <- blocking(Task {
          // TODO: this will no longer be needed once this PR makes it into a public release:
          // https://github.com/apple/foundationdb/pull/2635
          val m = classOf[FDB].getDeclaredMethod("selectAPIVersion", Integer.TYPE, java.lang.Boolean.TYPE)
          m.setAccessible(true)
          val fdb = m.invoke(null, 620, false).asInstanceOf[FDB]
          config.clusterFilePath.fold(fdb.open)(fdb.open)
        }.orDie)
          .log("Open FDB database")
        prefixMap <- buildPrefixMap(db, materialization)
          .log("Build FDB directory map")
      } yield FdbContext[BCF](db, prefixMap)
    } { dbContext =>
      blocking {
        Task {
          dbContext.db.close()
          FDB.instance().stopNetwork()
        }.orDie
      }.log("Close FDB database")
    }

    for {
      dbContext <- managedContext
      rt <- ZManaged.fromEffect(ZIO.runtime[AkkaEnv])
    } yield new FdbDatabase(materialization, dbContext, config)(rt)
  }

  val COMPLETED_FUTURE: CompletableFuture[Unit] = CompletableFuture.completedFuture(())
}

import dev.chopsticks.kvdb.fdb.FdbDatabase._

final class FdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] private (
  val materialization: KvdbMaterialization[BCF, CFS],
  dbContext: FdbContext[BCF],
  config: Config
)(implicit rt: zio.Runtime[AkkaEnv])
    extends KvdbDatabase[BCF, CFS]
    with StrictLogging {
  override def statsTask: Task[Map[(String, Map[String, String]), Double]] = ???

  private def doGet[Col <: CF](
    tx: ReadTransaction,
    column: Col,
    constraints: List[KvdbKeyConstraint]
  ): CompletableFuture[_ <: Either[Array[Byte], KvdbPair]] = {
    if (constraints.isEmpty) {
      CompletableFuture.completedFuture(Left(Array.emptyByteArray))
    }
    else {
      val prefixedConstraints = dbContext.prefixKeyConstraints(column, constraints)
      val headConstraint = prefixedConstraints.head
      val headOperand = headConstraint.operand.toByteArray
      val operator = headConstraint.operator

      operator match {
        case Operator.EQUAL =>
          tx.get(headOperand).thenApply { value =>
            if (keySatisfies(headOperand, prefixedConstraints.tail)) {
              Right((headOperand, value))
            }
            else Left(headOperand)
          }

        case _ =>
          val keyFuture = operator match {
            case Operator.PREFIX =>
              tx.getKey(KeySelector.firstGreaterOrEqual(headOperand))
            case Operator.GREATER =>
              tx.getKey(KeySelector.firstGreaterThan(headOperand))
            case Operator.LESS =>
              tx.getKey(KeySelector.lastLessThan(headOperand))
            case Operator.GREATER_EQUAL =>
              tx.getKey(KeySelector.firstGreaterOrEqual(headOperand))
            case Operator.LESS_EQUAL =>
              tx.getKey(KeySelector.lastLessOrEqual(headOperand))
            case Operator.FIRST =>
              tx.getKey(KeySelector.firstGreaterOrEqual(headOperand))
            case Operator.LAST =>
              tx.getKey(KeySelector.lastLessOrEqual(ByteArrayUtil.strinc(headOperand)))
            case o =>
              CompletableFuture.failedFuture(new IllegalArgumentException(s"Unrecognized operator: $o"))
          }

          val tailConstraints = prefixedConstraints.tail
          val ret: CompletableFuture[_ <: Either[Array[Byte], KvdbPair]] = keyFuture.thenComposeAsync {
            key: Array[Byte] =>
              if (KeySerdes.isPrefix(dbContext.columnPrefix(column), key) && keySatisfies(key, tailConstraints)) {
                tx.get(key).thenApply(value => Either.right[Array[Byte], KvdbPair]((key, value)))
              }
              else {
                CompletableFuture.completedFuture(Either.left[Array[Byte], KvdbPair](key))
              }
          }

          ret
      }

    }
  }

  override def getTask[Col <: CF](
    column: Col,
    constraints: KvdbKeyConstraintList
  ): Task[Option[(Array[Byte], Array[Byte])]] = {
    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          doGet(tx, column, constraints.constraints).thenApply { //noinspection MatchToPartialFunction
            result =>
              result match {
                case Right((key, value)) =>
                  Some(dbContext.unprefixKey(column, key) -> value)
                case _ => None
              }
          }
        }
      }
  }

  override def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]] = {
    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          val futures = requests.map { req =>
            doGet(tx, column, req.constraints)
              .thenApply { //noinspection MatchToPartialFunction
                result =>
                  result match {
                    case Right((key, value)) => Some(dbContext.unprefixKey(column, key) -> value)
                    case _ => None
                  }
              }
          }

          CompletableFuture
            .allOf(futures: _*)
            .thenApply(_ => futures.map(_.join()))
        }
      }
  }

  override def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          tx.set(prefixedKey, value)
          COMPLETED_FUTURE
        }
      }
  }

  override def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          tx.clear(prefixedKey)
          COMPLETED_FUTURE
        }
      }
  }

  override def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long] = {
    val prefixedKey = dbContext.prefixKey(column, prefix)

    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          tx.clear(com.apple.foundationdb.Range.startsWith(prefixedKey))
          COMPLETED_FUTURE
        }
      }
      .as(0L)
  }

  override def estimateCount[Col <: CF](column: Col): Task[Long] = ???

  override def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val tx = dbContext.db.createTransaction()
        val closeTx = () => tx.close()

        val future: Future[Source[KvdbBatch, NotUsed]] = doGet(tx, column, range.from).thenApply {
          //noinspection MatchToPartialFunction
          result =>
            val fromConstraints = dbContext.prefixKeyConstraints(column, range.from)
            val toConstraints = dbContext.prefixKeyConstraints(column, range.to)

            result match {
              case Right((key, _)) if keySatisfies(key, toConstraints) =>
                val toConstraintHead = toConstraints.head
                val toConstraintOperand = toConstraintHead.operand.toByteArray

                val endKeySelector = toConstraintHead.operator match {
                  case Operator.EQUAL =>
                    KeySelector.firstGreaterOrEqual(toConstraintOperand)
                  case Operator.LESS =>
                    KeySelector.firstGreaterOrEqual(toConstraintOperand)
                  case Operator.LESS_EQUAL =>
                    KeySelector.firstGreaterThan(toConstraintOperand)
                  case Operator.PREFIX =>
                    KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(toConstraintOperand))
                  case _ =>
                    KeySelector.firstGreaterOrEqual(dbContext.strinc(column))
                }

                val iterator = tx.snapshot().getRange(KeySelector.firstGreaterOrEqual(key), endKeySelector).iterator()
                val keyValidator = keySatisfies(_: Array[Byte], toConstraints)
                val keyTransformer = dbContext.unprefixKey(column, _: Array[Byte])

                Source
                  .fromGraph(
                    new FdbAsyncIteratorToSourceStage(
                      iterator,
                      keyValidator,
                      keyTransformer,
                      closeTx,
                      dbContext.dbCloseSignal
                    )
                  )

              case Right((k, _)) =>
                closeTx()
                val message =
                  s"Starting key: [${ByteArrayUtil.printable(k)}] satisfies fromConstraints ${fromConstraints.show} " +
                    s"but does not satisfy toConstraint: ${toConstraints.show}"
                Source.failed(SeekFailure(message))

              case Left(k) =>
                closeTx()
                val message = {
                  if (k.nonEmpty) {
                    s"Starting key: [${ByteArrayUtil.printable(k)}] does not satisfy constraints: ${fromConstraints.show}"
                  }
                  else s"There's no starting key satisfying constraint: ${fromConstraints.show}"
                }

                Source.failed(SeekFailure(message))
            }
        }.asScala

        future
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def iterateValuesSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbValueBatch, NotUsed] = ???

  override def transactionTask(
    actions: Seq[ColumnFamilyTransactionBuilder.TransactionAction],
    sync: Boolean
  ): Task[Unit] = {
    Task
      .fromCompletionStage {
        dbContext.db.runAsync { tx =>
          actions.foreach {
            case TransactionPut(columnId, key, value) =>
              tx.set(dbContext.prefixKey(columnId, key), value)
            case TransactionDelete(columnId, key, _) =>
              tx.clear(dbContext.prefixKey(columnId, key))
            case TransactionDeleteRange(columnId, fromKey, toKey) =>
              tx.clear(dbContext.prefixKey(columnId, fromKey), dbContext.prefixKey(columnId, toKey))
          }
          COMPLETED_FUTURE
        }
      }
  }

  override def tailSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbTailBatch, NotUsed] = ???

  override def tailValueSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbTailValueBatch, NotUsed] = ???

  override def concurrentTailSource[Col <: CF](column: Col, ranges: List[KvdbKeyRange])(
    implicit clientOptions: KvdbClientOptions
  ): Source[(Int, KvdbTailBatch), NotUsed] = ???

  override def dropColumnFamily[Col <: CF](column: Col): Task[Unit] = ???
}
