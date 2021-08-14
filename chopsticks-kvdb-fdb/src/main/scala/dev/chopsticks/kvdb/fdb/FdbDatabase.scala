package dev.chopsticks.kvdb.fdb

import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import cats.syntax.show._
import com.apple.foundationdb._
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
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
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbException._
import dev.chopsticks.kvdb.util.{KvdbCloseSignal, KvdbIoThreadPool}
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import org.reactivestreams.Publisher
import pureconfig.ConfigConvert
import zio._
import zio.blocking.{effectBlocking, Blocking}
import zio.clock.Clock
import zio.duration.Duration
import zio.interop.reactivestreams.Adapters.{createSubscription, demandUnfoldSink}

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.Instant
import java.util
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future, TimeoutException}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.Failure

object FdbDatabase {
  final case class FdbContext[BCF[A, B] <: ColumnFamily[A, B]](
    db: Database,
    prefixMap: Map[BCF[_, _], Array[Byte]],
    withVersionstampKeySet: Set[BCF[_, _]],
    withVersionstampValueSet: Set[BCF[_, _]],
    uuid: UUID
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
          Task.fail(KvdbAlreadyClosedException("Database was already closed")).unless(isClosed)
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

    def columnPrefix(columnId: String): Array[Byte] = {
      prefixMapById(columnId)
    }

    def columnPrefix[CF <: BCF[_, _]](column: CF): Array[Byte] = {
      columnPrefix(column.id)
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

    def unprefixKey(columnId: String, key: Array[Byte]): Array[Byte] = {
      val prefixLength = prefixMapById(columnId).length
      util.Arrays.copyOfRange(key, prefixLength, key.length)
    }

    def unprefixKey[CF <: BCF[_, _]](column: CF, key: Array[Byte]): Array[Byte] = {
      unprefixKey(column.id, key)
    }

    def prefixKeyConstraints(
      columnId: String,
      constraints: List[KvdbKeyConstraint]
    ): List[KvdbKeyConstraint] = {
      val prefix = prefixMapById(columnId)
      constraints.map { constraint =>
        constraint
          .copy(
            operand = ByteString.copyFrom(prefix).concat(constraint.operand),
            operandDisplay =
              s"[columnId=${columnId}][columnPrefix=${ByteArrayUtil.printable(prefix)}][operand=${constraint.operandDisplay}]"
          )
      }
    }

    def prefixKeyConstraints[CF <: BCF[_, _]](
      column: CF,
      constraints: List[KvdbKeyConstraint]
    ): List[KvdbKeyConstraint] = {
      prefixKeyConstraints(column.id, constraints)
    }
  }

  final case class FdbDatabaseConfig(
    clusterFilePath: Option[String],
    rootDirectoryPath: String,
    datacenterId: Option[String] = None,
    stopNetworkOnClose: Boolean = true,
    apiVersion: Int = 620,
    initialConnectionTimeout: Timeout = Timeout(5.seconds),
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
              .thenApply { dir =>
                cf -> dir.pack()
              }
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
  ): RManaged[Blocking with MeasuredLogging with KvdbIoThreadPool, Database] = {
    for {
      ioThreadPool <- ZManaged.access[KvdbIoThreadPool](_.get)
      ec = ioThreadPool.executor.asEC
      executor = new ExecutionContextExecutor {
        override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)
        override def execute(command: Runnable): Unit = ec.execute(command)
      }
      clusterFilePath <- config.clusterFilePath match {
        case Some(path) =>
          ZManaged.make {
            effectBlocking {
              Files.writeString(Files.createTempFile("fdb-connection", ".fdb"), Files.readString(Paths.get(path)))
            }
          } { path =>
            effectBlocking(Files.delete(path)).orDie
          }
            .map(Option(_))
        case _ =>
          ZManaged.succeed(None)
      }
      db <- Managed.make {
        effectBlocking {
          val fdb = FDB.selectAPIVersion(config.apiVersion)
          fdb.disableShutdownHook()

          val db = clusterFilePath.fold(fdb.open(null, executor))(path => fdb.open(path.toString, executor))
          config.datacenterId.foreach(dcid => db.options().setDatacenterId(dcid))
          db
        }.orDie
          .log("Open FDB database")
      } { db =>
        effectBlocking {
          db.close()
          if (config.stopNetworkOnClose) {
            FDB.instance().stopNetwork()
          }
        }
          .orDie.log("Close FDB database")
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
              config.initialConnectionTimeout.duration
            )
            .log("Build FDB directory map")
            .map { prefixMap =>
              FdbContext[BCF](
                db = db,
                prefixMap = prefixMap,
                withVersionstampKeySet = materialization.keyspacesWithVersionstampKey.map(_.keyspace),
                withVersionstampValueSet = materialization.keyspacesWithVersionstampValue.map(_.keyspace),
                uuid = UUID.randomUUID()
              )
            }
        } { db =>
          db.close()
            .log("Close FDB context")
            .orDie
        }
      db <- ZManaged.fromEffect(ZIO.runtime[AkkaEnv with MeasuredLogging].map { implicit rt =>
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
  val dbContext: FdbContext[BCF],
  ops: FdbOperations[BCF] = new FdbDefaultOperations[BCF]
)(implicit rt: zio.Runtime[AkkaEnv with MeasuredLogging])
    extends KvdbDatabase[BCF, CFS]
    with StrictLogging {

  override def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[BCF, CFS] = {
    val newOptions = modifier(clientOptions)
    new FdbDatabase[BCF, CFS](materialization, newOptions, dbContext, ops)
  }

  def withOps(modifier: FdbOperations[BCF] => FdbOperations[BCF]): FdbDatabase[BCF, CFS] = {
    new FdbDatabase[BCF, CFS](materialization, clientOptions, dbContext, modifier(ops))
  }

  override def statsTask: Task[Map[(String, Map[String, String]), Double]] = Task {
    Map(
      ("timestamp", Map.empty[String, String]) -> Instant.now.toEpochMilli.toDouble
    )
  }

  def read[V](fn: FdbReadApi[BCF] => CompletableFuture[V]): Task[V] = {
    TaskUtils
      .fromCancellableCompletableFuture(
        dbContext.db.readAsync { tx =>
          ops.read[V](new FdbReadApi[BCF](if (clientOptions.useSnapshotReads) tx.snapshot() else tx, dbContext), fn)
        }
      )
  }

  def write[V](name: => String, fn: FdbWriteApi[BCF] => CompletableFuture[V]): RIO[MeasuredLogging, V] = {
    TaskUtils
      .fromUninterruptibleCompletableFuture(
        name, {
          val tx = dbContext.db.createTransaction()
          val resultFuture = ops.write[V](
            new FdbWriteApi[BCF](
              tx,
              dbContext,
              clientOptions.disableWriteConflictChecking,
              clientOptions.useSnapshotReads
            ),
            fn
          )
          tx.commit().thenCompose(_ => resultFuture)
        }
      )
      .retry(
        Schedule
          .recurs(clientOptions.writeMaxRetryCount.value)
          .whileInput[Throwable] {
            case ex: FDBException if ex.isRetryableNotCommitted => true
            case _ => false
          }
      )
  }

  override def watchKeySource[Col <: CF](
    column: Col,
    key: Array[Byte]
  ): Source[Option[Array[Byte]], Future[Done]] = {
    val prefixedKey = dbContext.prefixKey(column, key)

    val watchTimeout = zio.duration.Duration.fromScala(clientOptions.watchTimeout)
    val watchMinLatency = clientOptions.watchMinLatency
    val watchMinLatencyNanos = watchMinLatency.toNanos

    Source
      .lazySource(() => {
        val zStream = zio.stream.Stream
          .repeatEffect {
            TaskUtils
              .fromCancellableCompletableFuture(
                dbContext.db.runAsync { tx =>
                  tx.get(prefixedKey).thenApply { value =>
                    val maybeValue = Option(value)
                    val watchCompletableFuture = tx.watch(prefixedKey)
                    (watchCompletableFuture, maybeValue)
                  }
                }: CompletableFuture[(CompletableFuture[Void], Option[Array[Byte]])]
              )
          }
          .flatMap { case (future, maybeValue) =>
            val watchTask = TaskUtils
              .fromCancellableCompletableFuture(future)
//              .log("watch task")
              .unit

            val watchTaskWithTimeout = watchTimeout match {
              case Duration.Infinity => watchTask
              case d => watchTask.timeoutTo(())(identity)(d)
            }

            val watchTaskWithRecovery = watchTaskWithTimeout
              .catchSome {
                case e: FDBException if e.getCode == 1009 => // Request for future version
                  ZIO.unit
                case e: FDBException =>
                  logger.warn(s"[watchKeySource][fdbErrorCode=${e.getCode}] ${e.getMessage}")
                  ZIO.unit
              }
              .timed
              .flatMap { case (elapsed, _) =>
                val elapsedNanos = elapsed.toNanos
                ZIO
                  .unit
                  .delay(java.time.Duration.ofNanos(watchMinLatencyNanos - elapsedNanos))
                  .when(elapsedNanos < watchMinLatencyNanos)
              }
              .as(Left(()))

            zio
              .stream
              .Stream(Right(maybeValue))
              .merge(zio.stream.Stream.fromEffect(
                watchTaskWithRecovery
              ))
          }
          .collect { case Right(maybeValue) => maybeValue }
          .changes

        val promise = scala.concurrent.Promise[Done]()
        val publisher: Publisher[Option[Array[Byte]]] = subscriber => {
          if (subscriber == null) {
            throw new NullPointerException("Subscriber must not be null.")
          }
          else {
            rt.unsafeRunAsync_(
              for {
                demand <- Queue.unbounded[Long]
                _ <- UIO(subscriber.onSubscribe(createSubscription(subscriber, demand, rt)))
                _ <- zStream
                  .run(demandUnfoldSink(subscriber, demand))
                  .catchAll(e => UIO(subscriber.onError(e)))
                  .onExit { exit: Exit[Throwable, Unit] =>
                    exit.foldM(
                      cause => UIO(promise.failure(cause.squash)),
                      _ => UIO(promise.success(Done))
                    )
                  }
                  .forkDaemon
              } yield ()
            )
          }
        }

        Source
          .fromPublisher(publisher)
          .mapMaterializedValue(_ => promise.future)
      })
      .mapMaterializedValue(future => future.flatten)
  }

  override def getTask[Col <: CF](
    column: Col,
    constraints: KvdbKeyConstraintList
  ): Task[Option[(Array[Byte], Array[Byte])]] = {
    read(_.get(column, constraints.constraints))
  }

  override def getRangeTask[Col <: CF](column: Col, range: KvdbKeyRange): Task[List[KvdbPair]] = {
    read(_.getRange(column, range))
  }

  override def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]] = {
    read { api =>
      val futures = requests.map { req =>
        api.get(column, req.constraints)
      }

      CompletableFuture
        .allOf(futures: _*)
        .thenApply(_ => futures.map(_.join()))
    }
  }

  override def batchGetRangeTask[Col <: CF](column: Col, ranges: Seq[KvdbKeyRange]): Task[List[List[KvdbPair]]] = {
    if (!ranges.forall(_.limit > 0)) {
      Task.fail(
        InvalidKvdbArgumentException(s"ranges contains non-positive integers")
      )
    }
    else {
      read { api =>
        val futures = ranges.view.map { range =>
          api.getRange(column, range)
        }.toList

        CompletableFuture
          .allOf(futures: _*)
          .thenApply(_ => futures.map(_.join()))
      }
    }
  }

  override def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): RIO[MeasuredLogging, Unit] = {
    write(
      s"putTask column=${column.id}",
      api => {
        api.put(column, key, value)
        COMPLETED_FUTURE
      }
    )
  }

  override def deleteTask[Col <: CF](column: Col, key: Array[Byte]): RIO[MeasuredLogging, Unit] = {
    write(
      s"deleteTask column=${column.id}",
      api => {
        api.delete(column, key)
        COMPLETED_FUTURE
      }
    )
  }

  override def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): RIO[MeasuredLogging, Long] = {
    write(
      s"deletePrefixTask column=${column.id}",
      api => {
        api.deletePrefix(column, prefix)
        COMPLETED_FUTURE
      }
    ).as(0L)
  }

  override def estimateCount[Col <: CF](column: Col): Task[Long] = ???

  override def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbBatch, NotUsed] = {
    Source
      .lazyFutureSource { () =>
        val initialTx = dbContext.db.createTransaction()
        val initialApi = new FdbReadApi[BCF](initialTx, dbContext)
        val closeTx = () => initialTx.close()

        val future: Future[Source[KvdbBatch, NotUsed]] =
          ops.read(initialApi, _.getEither(column, range.from)).thenApply { result =>
            val fromConstraints = dbContext.prefixKeyConstraints(column, range.from)
            val toConstraints = dbContext.prefixKeyConstraints(column, range.to)

            result match {
              case Right((key, _)) if keySatisfies(key, toConstraints) =>
                val keyValidator = keySatisfies(_: Array[Byte], toConstraints)
                val keyTransformer = dbContext.unprefixKey(column, _: Array[Byte])

                val iterate = (firstRun: Boolean, newRange: KvdbKeyRange) => {
                  val fromHead = newRange.from.head
                  val fromOperator = fromHead.operator
                  val fromOperand = fromHead.operand.toByteArray
                  val startKeySelector = fromOperator match {
                    case Operator.EQUAL => KeySelector.firstGreaterOrEqual(fromOperand)
                    case _ => initialApi.nonEqualFromConstraintToKeySelector(fromOperator, fromOperand)
                  }
                  val toHead = toConstraints.head
                  val endKeySelector = initialApi.toConstraintToKeySelector(
                    toHead.operator,
                    toHead.operand.toByteArray,
                    dbContext.strinc(column)
                  )
                  val tx = if (firstRun) initialTx else dbContext.db.createTransaction()
                  val closeTx = () => tx.close()
                  val iterator = ops.iterate(
                    new FdbReadApi[BCF](tx, dbContext),
                    _.tx.snapshot().getRange(startKeySelector, endKeySelector).iterator()
                  )
                  iterator -> closeTx
                }

                Source
                  .fromGraph(
                    new FdbIterateSourceStage(
                      initialRange = KvdbKeyRange(fromConstraints, toConstraints),
                      iterate = iterate,
                      keyValidator = keyValidator,
                      keyTransformer = keyTransformer,
                      shutdownSignal = dbContext.dbCloseSignal,
                      maxBatchBytes = clientOptions.batchReadMaxBatchBytes,
                      disableIsolationGuarantee = clientOptions.disableIsolationGuarantee
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
      }
      .mapMaterializedValue(_ => NotUsed)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def transactionTask(
    actions: Seq[TransactionWrite]
  ): RIO[MeasuredLogging, Unit] = {
    write(
      "transactionTask",
      api => {
//        api.tx.options().setReadYourWritesDisable()

        actions.foreach {
          case TransactionPut(columnId, key, value) =>
            api.putByColumnId(columnId, key, value)

          case TransactionDelete(columnId, key) =>
            api.deleteByColumnId(columnId, key)

          case TransactionDeleteRange(columnId, fromKey, toKey) =>
            api.deleteRangeByColumnId(columnId, fromKey, toKey)
        }

        COMPLETED_FUTURE
      }
    )
  }

  override def conditionalTransactionTask(
    reads: List[TransactionGet],
    condition: List[Option[(Array[Byte], Array[Byte])]] => Boolean,
    actions: Seq[TransactionWrite]
  ): RIO[MeasuredLogging, Unit] = {
    write(
      "conditionalTransactionTask",
      api => {
        val readFutures = for (read <- reads) yield {
          val key = read.key
          val f: CompletableFuture[Option[KvdbPair]] = api
            .tx
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
              actions.foreach {
                case TransactionPut(columnId, key, value) =>
                  api.putByColumnId(columnId, key, value)

                case TransactionDelete(columnId, key) =>
                  api.deleteByColumnId(columnId, key)

                case TransactionDeleteRange(columnId, fromKey, toKey) =>
                  api.deleteRangeByColumnId(columnId, fromKey, toKey)
              }

              COMPLETED_FUTURE
            }
            else {
              CompletableFuture.failedFuture(ConditionalTransactionFailedException("Condition returns false"))
            }
          }

        future

      }
    )
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
            val tx = dbContext.db.createTransaction()
            val api = new FdbReadApi[BCF](tx, dbContext)

            val startKeySelector = fromOperator match {
              case Operator.EQUAL =>
                KeySelector.firstGreaterOrEqual(fromOperand)
              case _ =>
                api.nonEqualFromConstraintToKeySelector(fromOperator, fromOperand)
            }

            val toHead = newRange.to.head
            val endKeySelector =
              api.toConstraintToKeySelector(toHead.operator, toHead.operand.toByteArray, dbContext.strinc(column))

            val closeTx = () => tx.close()
            val iterator = ops.iterate(api, _.tx.snapshot().getRange(startKeySelector, endKeySelector).iterator())

            iterator -> closeTx
          }

          val keyValidator = keySatisfies(_: Array[Byte], toConstraints)
          val keyTransformer = dbContext.unprefixKey(column, _: Array[Byte])

          Source
            .fromGraph(
              new FdbTailSourceStage(
                initialRange = KvdbKeyRange(fromConstraints, toConstraints),
                iterate = iterate,
                keyValidator = keyValidator,
                keyTransformer = keyTransformer,
                shutdownSignal = dbContext.dbCloseSignal,
                maxBatchBytes = clientOptions.batchReadMaxBatchBytes,
                tailPollingInterval = clientOptions.tailPollingMaxInterval,
                tailPollingBackoffFactor = clientOptions.tailPollingBackoffFactor
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

  override def dropColumnFamily[Col <: CF](column: Col): RIO[MeasuredLogging, Unit] = {
    write(
      "dropColumnFamily",
      api => {
        val prefix = dbContext.columnPrefix(column)
        api.tx.clear(com.apple.foundationdb.Range.startsWith(prefix))
        COMPLETED_FUTURE
      }
    )
  }
}
