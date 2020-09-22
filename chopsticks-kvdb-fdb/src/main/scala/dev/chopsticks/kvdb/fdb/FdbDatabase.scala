package dev.chopsticks.kvdb.fdb

import java.nio.{ByteBuffer, ByteOrder}
import java.time.Instant
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Source}
import cats.syntax.either._
import cats.syntax.show._
import com.apple.foundationdb._
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.LoggingContext
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.util.TaskUtils
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.KvdbDatabase.{keySatisfies, KvdbClientOptions}
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.{
  TransactionDelete,
  TransactionDeleteRange,
  TransactionPut,
  TransactionWrite
}
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits._
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbException._
import dev.chopsticks.kvdb.util.{KvdbCloseSignal, KvdbIoThreadPool}
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import eu.timepit.refined.types.numeric.PosInt
import pureconfig.ConfigConvert
import zio._
import zio.blocking.{blocking, Blocking}
import zio.clock.Clock
import zio.duration.Duration

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future, TimeoutException}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success}

object FdbDatabase extends LoggingContext {
  final case class FdbContext[BCF[A, B] <: ColumnFamily[A, B]](
    db: Database,
    prefixMap: Map[BCF[_, _], Array[Byte]],
    withVersionstampKeySet: Set[BCF[_, _]],
    withVersionstampValueSet: Set[BCF[_, _]]
  ) {
    private val prefixMapById: Map[String, Array[Byte]] = prefixMap.map {
      case (k, v) =>
        k.id -> v
    }
    private val withVersionstampKeyIdSet = withVersionstampKeySet.map(_.id)
    private val withVersionstampValueIdSet = withVersionstampValueSet.map(_.id)

    val dbCloseSignal = new KvdbCloseSignal

    private val isClosed = new AtomicBoolean(false)

    def close(): ZIO[Blocking with Clock, Throwable, Unit] = {
      val task = for {
        _ <- Task(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
          if (isClosed) Task.unit
          else Task.fail(KvdbAlreadyClosedException("Database was already closed"))
        }
        _ <- Task(dbCloseSignal.tryComplete(Failure(KvdbAlreadyClosedException("Database was already closed"))))
        _ <- Task(dbCloseSignal.hasNoListeners)
          .repeat(
            Schedule
              .fixed(100.millis)
              .untilInput[Boolean](identity)
          )
      } yield ()

      task
    }

    def hasVersionstampKey[CF <: BCF[_, _]](column: CF): Boolean = {
      hasVersionstampKey(column.id)
    }

    def hasVersionstampKey(columnId: String): Boolean = {
      withVersionstampKeyIdSet.contains(columnId)
    }

    def hasVersionstampValue[CF <: BCF[_, _]](column: CF): Boolean = {
      hasVersionstampValue(column.id)
    }

    def hasVersionstampValue(columnId: String): Boolean = {
      withVersionstampValueIdSet.contains(columnId)
    }

    def columnPrefix[CF <: BCF[_, _]](column: CF): Array[Byte] = {
      prefixMapById(column.id)
    }

    def strinc[CF <: BCF[_, _]](column: CF): Array[Byte] = {
      ByteArrayUtil.strinc(prefixMapById(column.id))
    }

    // From com.apple.foundationdb.tuple.TupleUtil
    private def adjustVersionPosition520(packed: Array[Byte], delta: Int): Array[Byte] = {
      val offsetOffset = packed.length - Integer.BYTES
      val buffer = ByteBuffer.wrap(packed, offsetOffset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
      val versionPosition = buffer.getInt + delta
      if (versionPosition < 0)
        throw new IllegalArgumentException("Tuple has an incomplete version at a negative position")
      val _ = buffer
        .position(offsetOffset)
        .putInt(versionPosition)
      packed
    }

    def adjustKeyVersionstamp[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      adjustKeyVersionstamp(column.id, key)
    }

    def adjustKeyVersionstamp(columnId: String, key: Array[Byte]): Array[Byte] = {
      adjustVersionPosition520(key, prefixMapById(columnId).length)
    }

    def prefixKey(columnId: String, key: Array[Byte]): Array[Byte] = {
      val prefix = prefixMapById(columnId)
      ByteArrayUtil.join(prefix, key)
    }

    def prefixKey[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      prefixKey(column.id, key)
    }

    def unprefixKey[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      val prefixLength = prefixMapById(column.id).length
      util.Arrays.copyOfRange(key, prefixLength, key.length)
    }

    def prefixKeyConstraints[CF <: BCF[_, _]](
      column: CF,
      constraints: List[KvdbKeyConstraint]
    ): List[KvdbKeyConstraint] = {
      val prefix = prefixMapById(column.id)
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

  final case class FdbDatabaseConfig(
    clusterFilePath: Option[String],
    rootDirectoryPath: String,
    stopNetworkOnClose: Boolean = true,
    clientOptions: KvdbClientOptions = KvdbClientOptions()
  )

  object FdbDatabaseConfig {
    import dev.chopsticks.util.config.PureconfigConverters._
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[FdbDatabaseConfig]
  }

  private def buildPrefixMap[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    db: Database,
    materialization: KvdbMaterialization[BCF, CFS],
    config: FdbDatabaseConfig
  ): Task[Map[BCF[_, _], Array[Byte]]] = {
    Task.fromCompletionStage[Map[BCF[_, _], Array[Byte]]] {
      db.runAsync { tx =>
        val rootDirectoryPath = config.rootDirectoryPath
        val directoryFuture =
          if (rootDirectoryPath.isEmpty) {
            CompletableFuture.completedFuture(DirectoryLayer.getDefault)
          }
          else {
            DirectoryLayer.getDefault.createOrOpen(tx, List(rootDirectoryPath).asJava)
          }

        directoryFuture.thenCompose { directory =>
          val futures = materialization.columnFamilySet.value.toList.map { cf =>
            directory
              .createOrOpen(tx, List(cf.id).asJava)
              .thenApply(dir => cf -> dir.pack())
          }

          CompletableFuture
            .allOf(futures: _*)
            .thenApply(_ => futures.map(_.join()).toMap)
        }
      }
    }
  }

  def fromConfig(
    config: FdbDatabaseConfig
  ): ZManaged[Blocking with MeasuredLogging with KvdbIoThreadPool, Nothing, Database] = {
    for {
      ioThreadPool <- ZManaged.access[KvdbIoThreadPool](_.get)
      ec = ioThreadPool.executor.asEC
      executor = new ExecutionContextExecutor {
        override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)
        override def execute(command: Runnable): Unit = ec.execute(command)
      }
      db <- Managed.make {
        blocking(Task {
          // TODO: this will no longer be needed once this PR makes it into a public release:
          // https://github.com/apple/foundationdb/pull/2635
          val m = classOf[FDB].getDeclaredMethod("selectAPIVersion", Integer.TYPE, java.lang.Boolean.TYPE)
          m.setAccessible(true)
          val fdb = m.invoke(null, 620, false).asInstanceOf[FDB]
          config.clusterFilePath.fold(fdb.open(null, executor))(path => fdb.open(path, executor))
        }.orDie)
          .log("Open FDB database")
      } { db =>
        blocking {
          Task {
            db.close()
            if (config.stopNetworkOnClose) {
              FDB.instance().stopNetwork()
            }
          }.orDie
        }.log("Close FDB database")
      }
    } yield db
  }

  def fromDatabase[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS] with FdbMaterialization[BCF],
    db: Database,
    config: FdbDatabaseConfig
  ): ZManaged[AkkaEnv with Blocking with MeasuredLogging, Throwable, KvdbDatabase[BCF, CFS]] = {
    for {
      ctx <- ZManaged
        .makeInterruptible[AkkaEnv with Blocking with MeasuredLogging, Throwable, FdbContext[BCF]] {
          buildPrefixMap(db, materialization, config)
            .timeoutFail(new TimeoutException("Timed out building directory layer. Check connection to FDB?"))(
              2.seconds
            )
            .log("Build FDB directory map")
            .map { prefixMap =>
              FdbContext[BCF](
                db = db,
                prefixMap = prefixMap,
                withVersionstampKeySet = materialization.keyspacesWithVersionstampKey.map(_.keyspace),
                withVersionstampValueSet = materialization.keyspacesWithVersionstampValue.map(_.keyspace)
              )
            }
        } {
          _.close()
            .log("Close FDB context")
            .orDie
        }
      db <- ZManaged.fromEffect(ZIO.runtime[AkkaEnv with Clock].map { implicit rt =>
        new FdbDatabase(materialization, config.clientOptions, ctx)
      })
    } yield db
  }

  def manage[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS] with FdbMaterialization[BCF],
    config: FdbDatabaseConfig
  ): ZManaged[AkkaEnv with Blocking with MeasuredLogging with KvdbIoThreadPool, Throwable, KvdbDatabase[BCF, CFS]] = {
    for {
      db <- fromConfig(config)
      fdbDatabase <- fromDatabase(materialization, db, config)
    } yield fdbDatabase
  }

  val COMPLETED_FUTURE: CompletableFuture[Unit] = CompletableFuture.completedFuture(())
}

import dev.chopsticks.kvdb.fdb.FdbDatabase._

final class FdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] private (
  val materialization: KvdbMaterialization[BCF, CFS],
  val clientOptions: KvdbClientOptions,
  val dbContext: FdbContext[BCF]
)(implicit rt: zio.Runtime[AkkaEnv with Clock])
    extends KvdbDatabase[BCF, CFS]
    with StrictLogging {

  override def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[BCF, CFS] = {
    val newOptions = modifier(clientOptions)
    new FdbDatabase[BCF, CFS](materialization, newOptions, dbContext)
  }

  override def statsTask: Task[Map[(String, Map[String, String]), Double]] = Task {
    Map(
      ("timestamp", Map.empty[String, String]) -> Instant.now.toEpochMilli.toDouble
    )
  }

  private def nonEqualFromConstraintToKeySelector(operator: Operator, operand: Array[Byte]): KeySelector = {
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

  private def toConstraintToKeySelector(
    operator: Operator,
    operand: Array[Byte],
    firstOutOfRangeOperand: Array[Byte]
  ) = {
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
            if (value != null && keySatisfies(headOperand, prefixedConstraints.tail)) {
              Right((headOperand, value))
            }
            else Left(headOperand)
          }

        case op =>
          val keySelector = nonEqualFromConstraintToKeySelector(operator, headOperand)
          val keyFuture = tx.getKey(keySelector)
          val tailConstraints = if (op == Operator.PREFIX) prefixedConstraints else prefixedConstraints.tail

          val ret: CompletableFuture[_ <: Either[Array[Byte], KvdbPair]] = keyFuture.thenCompose { key: Array[Byte] =>
            if (
              key != null && key.length > 0 && KeySerdes.isPrefix(dbContext.columnPrefix(column), key) && keySatisfies(
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

  override def watchKeySource[Col <: CF](
    column: Col,
    key: Array[Byte]
  ): Source[Option[Array[Byte]], NotUsed] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    import dev.chopsticks.stream.ZAkkaStreams.ops._
    val watchTimeout = zio.duration.Duration.fromScala(clientOptions.watchTimeout)
    val watchMinLatency = clientOptions.watchMinLatency
    val watchMinLatencyNanos = watchMinLatency.toNanos

    Source
      .unfoldAsync(Future.successful(System.nanoTime())) { waitFuture =>
        import scala.jdk.FutureConverters._
        val akkaService = rt.environment.get
        val scheduler = akkaService.actorSystem.scheduler

        import akkaService.dispatcher

        waitFuture
          .transformWith { lastTry =>
            val txFuture = dbContext.db.runAsync { tx =>
              tx.get(prefixedKey).thenApply { value =>
                val maybeValue = Option(value)
                val watchCompletableFuture = tx.watch(prefixedKey)
                (watchCompletableFuture, maybeValue)
              }
            }: CompletableFuture[(CompletableFuture[Void], Option[Array[Byte]])]

            txFuture
              .asScala
              .map {
                case (future, maybeValue) =>
                  val watchTask = TaskUtils
                    .fromCancellableCompletableFuture(future)
                    .as(maybeValue)

                  val watchTaskWithTimeout = watchTimeout match {
                    case Duration.Infinity => watchTask
                    case d => watchTask.timeoutTo(maybeValue)(identity)(d)
                  }

                  val watchTaskWithRecovery = watchTaskWithTimeout
                    .catchSome {
                      case e: FDBException if e.getCode == 1009 => // Request for future version
                        ZIO.succeed(maybeValue)
                      case e: FDBException =>
                        logger.warn(s"[watchKeySource][fdbErrorCode=${e.getCode}] ${e.getMessage}")
                        ZIO.succeed(maybeValue)
                    }

                  val emit = Task.succeed(maybeValue) :: watchTaskWithRecovery :: Nil

                  val nextWaitFuture = future.asScala.flatMap { _ =>
                    val now = System.nanoTime()
                    val f = Future.successful(now)

                    lastTry match {
                      case Failure(_) =>
                        akka.pattern.after(watchMinLatency, scheduler)(f)
                      case Success(lastNanos) =>
                        val elapse = now - lastNanos

                        if (elapse < watchMinLatencyNanos) {
                          akka.pattern.after(
                            FiniteDuration(watchMinLatencyNanos - elapse, TimeUnit.NANOSECONDS),
                            scheduler
                          )(f)
                        }
                        else f
                    }
                  }

                  Some(nextWaitFuture -> emit)
              }
          }
      }
      .mapConcat(identity)
      .interruptibleEffectMapAsyncUnordered(1)(identity)
      .statefulMapConcat(() => {
        var isFirst = true
        var currentValue = Option.empty[Array[Byte]]

        value => {
          if (isFirst) {
            isFirst = false
            currentValue = value
            currentValue :: Nil
          }
          else {
            (currentValue, value) match {
              case (Some(a), Some(b)) if a.sameElements(b) => Nil
              case (None, None) => Nil
              case _ =>
                currentValue = value
                currentValue :: Nil
            }
          }
        }
      })
  }

  override def getTask[Col <: CF](
    column: Col,
    constraints: KvdbKeyConstraintList
  ): Task[Option[(Array[Byte], Array[Byte])]] = {
    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db.readAsync { tx =>
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

  private[chopsticks] def doGetRangeFuture[Col <: CF](
    tx: ReadTransaction,
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

  override def getRangeTask[Col <: CF](column: Col, range: KvdbKeyRange): Task[List[KvdbPair]] = {
    PosInt.from(range.limit) match {
      case Left(_) =>
        Task.fail(
          InvalidKvdbArgumentException(s"range.limit of '${range.limit}' is invalid, must be a positive integer")
        )
      case Right(limit) =>
        TaskUtils
          .fromCancellableCompletableFuture {
            dbContext.db
              .readAsync { tx =>
                doGetRangeFuture(tx, column, range.from, range.to, limit)
              }
          }
    }
  }

  override def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]] = {
    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .readAsync { tx =>
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

  override def batchGetRangeTask[Col <: CF](column: Col, ranges: Seq[KvdbKeyRange]): Task[List[List[KvdbPair]]] = {
    if (!ranges.forall(_.limit > 0)) {
      Task.fail(
        InvalidKvdbArgumentException(s"ranges contains non-positive integers")
      )
    }
    else {
      TaskUtils
        .fromCancellableCompletableFuture {
          dbContext.db
            .readAsync { tx =>
              val futures = ranges.view.map { range =>
                doGetRangeFuture(tx, column, range.from, range.to, PosInt.unsafeFrom(range.limit))
              }.toList

              CompletableFuture
                .allOf(futures: _*)
                .thenApply(_ => futures.map(_.join()))
            }
        }
    }
  }

  override def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            if (clientOptions.disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()

            if (dbContext.hasVersionstampKey(column)) {
              tx.mutate(
                MutationType.SET_VERSIONSTAMPED_KEY,
                dbContext.adjustKeyVersionstamp(column, prefixedKey),
                value
              )
            }
            else if (dbContext.hasVersionstampValue(column)) {
              tx.mutate(
                MutationType.SET_VERSIONSTAMPED_VALUE,
                prefixedKey,
                value
              )
            }
            else {
              tx.set(prefixedKey, value)
            }

            COMPLETED_FUTURE
          }
      }
  }

  override def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            tx.clear(prefixedKey)
            COMPLETED_FUTURE
          }
      }
  }

  override def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long] = {
    val prefixedKey = dbContext.prefixKey(column, prefix)

    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            tx.clear(com.apple.foundationdb.Range.startsWith(prefixedKey))
            COMPLETED_FUTURE
          }
      }
      .as(0L)
  }

  override def estimateCount[Col <: CF](column: Col): Task[Long] = ???

  override def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbBatch, NotUsed] = {
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
                val endKeySelector =
                  toConstraintToKeySelector(
                    toConstraintHead.operator,
                    toConstraintHead.operand.toByteArray,
                    dbContext.strinc(column)
                  )
                val iterator = tx.snapshot().getRange(KeySelector.firstGreaterOrEqual(key), endKeySelector).iterator()
                val keyValidator = keySatisfies(_: Array[Byte], toConstraints)
                val keyTransformer = dbContext.unprefixKey(column, _: Array[Byte])

                Source
                  .fromGraph(
                    new FdbIterateSourceStage(
                      iterator,
                      keyValidator,
                      keyTransformer,
                      closeTx,
                      dbContext.dbCloseSignal,
                      clientOptions.batchReadMaxBatchBytes
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

  override def transactionTask(
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            tx.options().setReadYourWritesDisable()

            val disableWriteConflictChecking = clientOptions.disableWriteConflictChecking

            actions.foreach { action =>
              if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()

              action match {
                case TransactionPut(columnId, key, value) =>
                  val prefixedKey = dbContext.prefixKey(columnId, key)

                  if (dbContext.hasVersionstampKey(columnId)) {
                    tx.mutate(
                      MutationType.SET_VERSIONSTAMPED_KEY,
                      dbContext.adjustKeyVersionstamp(columnId, prefixedKey),
                      value
                    )
                  }
                  else if (dbContext.hasVersionstampValue(columnId)) {
                    tx.mutate(
                      MutationType.SET_VERSIONSTAMPED_VALUE,
                      prefixedKey,
                      value
                    )
                  }
                  else {
                    tx.set(prefixedKey, value)
                  }

                case TransactionDelete(columnId, key) =>
                  tx.clear(dbContext.prefixKey(columnId, key))

                case TransactionDeleteRange(columnId, fromKey, toKey) =>
                  tx.clear(dbContext.prefixKey(columnId, fromKey), dbContext.prefixKey(columnId, toKey))
              }
            }

            COMPLETED_FUTURE
          }
      }
  }

  override def conditionalTransactionTask(
    reads: List[TransactionGet],
    condition: List[Option[(Array[Byte], Array[Byte])]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            val readFutures = for (read <- reads) yield {
              val key = read.key
              val f: CompletableFuture[Option[KvdbPair]] = tx
                .get(dbContext.prefixKey(read.columnId, key))
                .thenApply { value => if (value != null) Some(key -> value) else None }
              f
            }

            val future: CompletableFuture[Unit] = CompletableFuture
              .allOf(readFutures: _*)
              .thenCompose { _ =>
                val pairs = readFutures.map(_.join())

                val okToWrite = condition(pairs)

                if (okToWrite) {
                  val disableWriteConflictChecking = clientOptions.disableWriteConflictChecking

                  actions.foreach { action =>
                    if (disableWriteConflictChecking) tx.options().setNextWriteNoWriteConflictRange()

                    action match {
                      case TransactionPut(columnId, key, value) =>
                        val prefixedKey = dbContext.prefixKey(columnId, key)

                        if (dbContext.hasVersionstampKey(columnId)) {
                          tx.mutate(
                            MutationType.SET_VERSIONSTAMPED_KEY,
                            dbContext.adjustKeyVersionstamp(columnId, prefixedKey),
                            value
                          )
                        }
                        else if (dbContext.hasVersionstampValue(columnId)) {
                          tx.mutate(
                            MutationType.SET_VERSIONSTAMPED_VALUE,
                            prefixedKey,
                            value
                          )
                        }
                        else {
                          tx.set(prefixedKey, value)
                        }

                      case TransactionDelete(columnId, key) =>
                        tx.clear(dbContext.prefixKey(columnId, key))

                      case TransactionDeleteRange(columnId, fromKey, toKey) =>
                        tx.clear(dbContext.prefixKey(columnId, fromKey), dbContext.prefixKey(columnId, toKey))
                    }
                  }

                  COMPLETED_FUTURE
                }
                else {
                  CompletableFuture.failedFuture(ConditionalTransactionFailedException("Condition returns false"))
                }
              }

            future
          }
      }
  }

  override def tailSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbTailBatch, NotUsed] = {
    Source
      .lazySource(() => {
        if (range.from.isEmpty) {
          Source.failed(
            UnsupportedKvdbOperationException(
              "range.from cannot be empty for tailSource, since it can never be satisfied"
            )
          )
        }
        else {
          val fromConstraints = dbContext.prefixKeyConstraints(column, range.from)
          val toConstraints = dbContext.prefixKeyConstraints(column, range.to)

          val iterate = (newRange: KvdbKeyRange) => {
            val fromHead = newRange.from.head
            val fromOperator = fromHead.operator
            val fromOperand = fromHead.operand.toByteArray

            val startKeySelector = fromOperator match {
              case Operator.EQUAL =>
                KeySelector.firstGreaterOrEqual(fromOperand)
              case _ =>
                nonEqualFromConstraintToKeySelector(fromOperator, fromOperand)
            }

            val toHead = newRange.to.head
            val endKeySelector =
              toConstraintToKeySelector(toHead.operator, toHead.operand.toByteArray, dbContext.strinc(column))

            val tx = dbContext.db.createTransaction()
            val closeTx = () => tx.close()
            val iterator = tx.snapshot().getRange(startKeySelector, endKeySelector).iterator()

            iterator -> closeTx
          }

          val keyValidator = keySatisfies(_: Array[Byte], toConstraints)
          val keyTransformer = dbContext.unprefixKey(column, _: Array[Byte])

          Source
            .fromGraph(
              new FdbTailSourceStage(
                KvdbKeyRange(fromConstraints, toConstraints),
                iterate,
                keyValidator,
                keyTransformer,
                dbContext.dbCloseSignal,
                clientOptions.batchReadMaxBatchBytes,
                clientOptions.tailPollingMaxInterval,
                clientOptions.tailPollingBackoffFactor
              )
            )
        }
      })
      .mapMaterializedValue(_ => NotUsed)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def concurrentTailSource[Col <: CF](
    column: Col,
    ranges: List[KvdbKeyRange]
  ): Source[(Int, KvdbTailBatch), NotUsed] = {
    Source
      .lazySource(() => {
        if (ranges.exists(_.from.isEmpty)) {
          Source.failed(
            UnsupportedKvdbOperationException(
              "range.from cannot be empty for tailSource, since it can never be satisfied"
            )
          )
        }
        else {
          def tail(index: Int, range: KvdbKeyRange) = {
            tailSource(column, range)
              .map(b => (index, b))
          }

          ranges match {
            case Nil =>
              Source.failed(UnsupportedKvdbOperationException("ranges cannot be empty"))

            case head :: Nil =>
              tail(0, head)

            case head :: next :: rest =>
              Source.combine(
                tail(0, head),
                tail(1, next),
                rest.zipWithIndex.map { case (r, i) => tail(i + 2, r) }: _*
              )(Merge(_))
          }
        }
      })
      .mapMaterializedValue(_ => NotUsed)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def dropColumnFamily[Col <: CF](column: Col): Task[Unit] = {
    TaskUtils
      .fromCancellableCompletableFuture {
        dbContext.db
          .runAsync { tx =>
            val prefix = dbContext.columnPrefix(column)
            tx.clear(com.apple.foundationdb.Range.startsWith(prefix))
            COMPLETED_FUTURE
          }
      }
  }

}
