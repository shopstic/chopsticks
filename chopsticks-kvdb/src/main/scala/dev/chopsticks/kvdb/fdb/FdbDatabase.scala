package dev.chopsticks.kvdb.fdb

import cats.syntax.show.*
import com.apple.foundationdb.*
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.google.protobuf.ByteString
import dev.chopsticks.schema.SchemaAnnotation
import zio.stream.ZStream
import dev.chopsticks.fp.util.TaskUtils
import dev.chopsticks.fp.zio_ext.*
import dev.chopsticks.kvdb.KvdbDatabase.{keySatisfies, KvdbClientOptions}
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionRead
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder.TransactionWrite
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits.*
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto.{KvdbKeyConstraint, KvdbKeyConstraintList, KvdbKeyRange}
import dev.chopsticks.kvdb.util.KvdbAliases.*
import dev.chopsticks.kvdb.util.KvdbException.*
import dev.chopsticks.kvdb.util.{KvdbCloseSignal, KvdbIoThreadPool}
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import zio.*
import zio.stream.*

import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.Instant
import java.util
import java.util.UUID
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContextExecutor, TimeoutException}
import scala.jdk.CollectionConverters.*
import scala.util.Failure

object FdbDatabase {
  val defaultWriteRetrySchedule: Schedule[Any, Throwable, Any] = {
    Schedule
      .forever
      .whileInput[Throwable] {
        case ex: FDBException
            if (ex.isRetryable ||
              ex.getCode == 1007 /* Transaction too old */ ||
              ex.getCode == 1009 /* Request for future version */ ||
              ex.getCode == 1037 /* process_behind */ ) &&
              ex.getCode != 1021 /* commit_unknown_result */ => true
        case _ => false
      }
  }

  final case class FdbContext[CFS <: ColumnFamily[_, _]](
    db: Database,
    prefixMap: Map[ColumnFamily[_, _], Array[Byte]],
    withVersionstampKeySet: Set[ColumnFamily[_, _]],
    withVersionstampValueSet: Set[ColumnFamily[_, _]],
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

    def close(): ZIO[Any, Throwable, Unit] = {
      val task = for {
        _ <- ZIO.attempt(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
          ZIO.fail(KvdbAlreadyClosedException("Database was already closed")).unless(isClosed)
        }
        _ <- ZIO.attempt(dbCloseSignal.tryComplete(Failure(KvdbAlreadyClosedException("Database was already closed"))))
        _ <- ZIO.attempt(dbCloseSignal.hasNoListeners)
          .repeat(
            Schedule
              .fixed(100.millis)
              .untilInput[Boolean](identity)
          )
      } yield ()

      task
    }

    def hasVersionstampKey[CF <: ColumnFamily[_, _]](column: CF)(using CFS <:< CF): Boolean =
      hasVersionstampKey(column.id)

    def hasVersionstampKey(columnId: String): Boolean =
      withVersionstampKeyIdSet.contains(columnId)

    def hasVersionstampValue[CF <: ColumnFamily[_, _]](column: CF)(using CFS <:< CF): Boolean =
      hasVersionstampValue(column.id)

    def hasVersionstampValue(columnId: String): Boolean =
      withVersionstampValueIdSet.contains(columnId)

    def columnPrefix(columnId: String): Array[Byte] =
      prefixMapById(columnId)

    def columnPrefix[CF <: ColumnFamily[_, _]](column: CF)(using CFS <:< CF): Array[Byte] =
      columnPrefix(column.id)

    def strinc[CF <: ColumnFamily[_, _]](column: CF)(using CFS <:< CF): Array[Byte] =
      ByteArrayUtil.strinc(prefixMapById(column.id))

    // From com.apple.foundationdb.tuple.TupleUtil
    private def adjustVersionPosition520(packed: Array[Byte], delta: Int): Array[Byte] =
      val offsetOffset = packed.length - Integer.BYTES
      val buffer = ByteBuffer.wrap(packed, offsetOffset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
      val versionPosition = buffer.getInt + delta
      if (versionPosition < 0)
        throw new IllegalArgumentException("Tuple has an incomplete version at a negative position")
      val _ = buffer
        .position(offsetOffset)
        .putInt(versionPosition)
      packed

    def adjustKeyVersionstamp[CF <: ColumnFamily[_, _]](column: CF, key: Array[Byte])(using CFS <:< CF): Array[Byte] =
      adjustKeyVersionstamp(column.id, key)

    def adjustKeyVersionstamp(columnId: String, key: Array[Byte]): Array[Byte] =
      adjustVersionPosition520(key, prefixMapById(columnId).length)

    def prefixKey(columnId: String, key: Array[Byte]): Array[Byte] =
      val prefix = prefixMapById(columnId)
      ByteArrayUtil.join(prefix, key)

    def prefixKey[CF <: ColumnFamily[_, _]](column: CF, key: Array[Byte])(using CFS <:< CF): Array[Byte] =
      prefixKey(column.id, key)

    def unprefixKey(columnId: String, key: Array[Byte]): Array[Byte] =
      val prefixLength = prefixMapById(columnId).length
      util.Arrays.copyOfRange(key, prefixLength, key.length)

    def unprefixKey[CF <: ColumnFamily[_, _]](column: CF, key: Array[Byte])(using CFS <:< CF): Array[Byte] =
      unprefixKey(column.id, key)

    def prefixKeyConstraints(
      columnId: String,
      constraints: List[KvdbKeyConstraint]
    ): List[KvdbKeyConstraint] =
      val prefix = prefixMapById(columnId)
      constraints.map { constraint =>
        constraint
          .copy(
            operand = ByteString.copyFrom(prefix).concat(constraint.operand),
            operandDisplay =
              s"[columnId=${columnId}][columnPrefix=${ByteArrayUtil.printable(prefix)}][operand=${constraint.operandDisplay}]"
          )
      }

    def prefixKeyConstraints[CF <: ColumnFamily[_, _]](
      column: CF,
      constraints: List[KvdbKeyConstraint]
    )(using CFS <:< CF): List[KvdbKeyConstraint] = {
      prefixKeyConstraints(column.id, constraints)
    }
  }

  final case class FdbDatabaseConfig(
    @SchemaAnnotation.Default(Option.empty[String])
    clusterFilePath: Option[String],
    rootDirectoryPath: String,
    datacenterId: Option[String] = None,
    @SchemaAnnotation.Default(true)
    stopNetworkOnClose: Boolean = true,
    @SchemaAnnotation.Default[Int](710)
    apiVersion: Int = 710,
    @SchemaAnnotation.Default[Duration](Duration.fromSeconds(5))
    initialConnectionTimeout: Duration = Duration.fromSeconds(5),
    @SchemaAnnotation.Default[KvdbClientOptions](KvdbClientOptions())
    clientOptions: KvdbClientOptions = KvdbClientOptions()
  )
  object FdbDatabaseConfig extends dev.chopsticks.schema.config.SchemaConfig[FdbDatabaseConfig]:
    implicit override lazy val zioSchema: zio.schema.Schema[FdbDatabaseConfig] = zio.schema.DeriveSchema.gen

  private def buildPrefixMap[CFS <: ColumnFamily[_, _]](
    db: Database,
    materialization: KvdbMaterialization[CFS],
    config: FdbDatabaseConfig
  ): Task[Map[ColumnFamily[_, _], Array[Byte]]] = {
    ZIO.fromCompletionStage[Map[ColumnFamily[_, _], Array[Byte]]] {
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

  private lazy val fdb: FDB =
    val db = FDB.selectAPIVersion(710)
    db.disableShutdownHook()
    db

  def fromConfig(
    config: FdbDatabaseConfig
  ): RIO[KvdbIoThreadPool with Scope, Database] =
    for {
      ioThreadPool <- ZIO.service[KvdbIoThreadPool]
      ec = ioThreadPool.executor.asExecutionContext
      executor = new ExecutionContextExecutor:
        override def reportFailure(cause: Throwable): Unit = ec.reportFailure(cause)
        override def execute(command: Runnable): Unit = ec.execute(command)
      clusterFilePath <- config.clusterFilePath match {
        case Some(path) =>
          ZIO.acquireRelease {
            ZIO.attemptBlocking {
              Files.writeString(Files.createTempFile("fdb-connection", ".fdb"), Files.readString(Paths.get(path)))
            }
          } { path =>
            ZIO.attemptBlocking(Files.delete(path)).orDie
          }
            .map(Option(_))
        case _ =>
          ZIO.succeed(None)
      }
      db <- ZIO.acquireRelease {
        ZIO.suspend {
          val clientThreadCount =
            math.max(1, sys.env.getOrElse("FDB_NETWORK_OPTION_CLIENT_THREADS_PER_VERSION", "1").toInt)

          ZIO
            .attemptBlocking {
              // set the knobs before the client is started
              config.clientOptions.knobs.valueSizeLimit.foreach { limit =>
                fdb.options().setKnob(s"value_size_limit=${limit.toBytes.toLong.toString}")
              }

              val pool = ArraySeq.tabulate(clientThreadCount)(_ =>
                clusterFilePath.fold(fdb.open(null, executor))(path => fdb.open(path.toString, executor))
              )

              val db =
                if (clientThreadCount == 1) pool.head
                else new FdbPooledDatabase(pool)

              config.datacenterId.foreach(dcid => db.options().setDatacenterId(dcid))

              db
            }
            .orDie
            .log(s"Open FDB database client_thread_count=$clientThreadCount")
        }
      } { db =>
        ZIO
          .attemptBlocking {
            db.close()
            if (config.stopNetworkOnClose) {
              FDB.instance().stopNetwork()
            }
          }
          .orDie
          .log("Close FDB database")
      }
    } yield db

  def fromDatabase[CF <: ColumnFamily[_, _]](
    materialization: KvdbMaterialization[CF] with FdbMaterialization,
    db: Database,
    config: FdbDatabaseConfig
  ): ZIO[Scope, Throwable, KvdbDatabase[CF]] =
    for {
      ctx <- buildPrefixMap(db, materialization, config)
        .timeoutFail(new TimeoutException("Timed out building directory layer. Check connection to FDB?"))(
          config.initialConnectionTimeout
        )
        .log("Build FDB directory map")
        .map { prefixMap =>
          FdbContext[CF](
            db = db,
            prefixMap = prefixMap,
            withVersionstampKeySet = materialization.keyspacesWithVersionstampKey.map(_.keyspace),
            withVersionstampValueSet = materialization.keyspacesWithVersionstampValue.map(_.keyspace),
            uuid = UUID.randomUUID()
          )
        }
      _ <- ZIO.addFinalizer {
        ctx.close()
          .log("Close FDB context")
          .orDie
      }
      db <- ZIO.runtime[Any].map { implicit rt =>
        new FdbDatabase(materialization, config.clientOptions, ctx)
      }
    } yield db

  def manage[CFS <: ColumnFamily[_, _]](
    materialization: KvdbMaterialization[CFS] with FdbMaterialization,
    config: FdbDatabaseConfig
  ): ZIO[KvdbIoThreadPool with Scope, Throwable, KvdbDatabase[CFS]] = {
    for {
      db <- fromConfig(config)
      fdbDatabase <- fromDatabase(materialization, db, config)
    } yield fdbDatabase
  }

  val COMPLETED_FUTURE: CompletableFuture[Unit] = CompletableFuture.completedFuture(())
  private val NOOP_CALLBACK = () => ()
}

import dev.chopsticks.kvdb.fdb.FdbDatabase.*

final class FdbDatabase[CFS <: ColumnFamily[_, _]] private (
  val materialization: KvdbMaterialization[CFS],
  val clientOptions: KvdbClientOptions,
  val dbContext: FdbContext[CFS],
  ops: FdbOperations[CFS] = new FdbDefaultOperations[CFS]
)(implicit rt: zio.Runtime[Any])
    extends KvdbDatabase[CFS] {

  override def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[CFS] =
    val newOptions = modifier(clientOptions)
    new FdbDatabase[CFS](materialization, newOptions, dbContext, ops)

  def withOps(modifier: FdbOperations[CFS] => FdbOperations[CFS]): FdbDatabase[CFS] =
    new FdbDatabase[CFS](materialization, clientOptions, dbContext, modifier(ops))

  override def statsTask: Task[Map[(String, Map[String, String]), Double]] =
    ZIO.attempt {
      Map(
        ("timestamp", Map.empty[String, String]) -> Instant.now.toEpochMilli.toDouble
      )
    }

  def uninterruptibleRead[V](fn: FdbReadApi[CFS] => CompletableFuture[V]): Task[V] =
    ZIO
      .fromCompletableFuture {
        dbContext.db
          .readAsync { tx =>
            ops
              .read[V](new FdbReadApi[CFS](if (clientOptions.useSnapshotReads) tx.snapshot() else tx, dbContext), fn)
          }
          .orTimeout(6, TimeUnit.SECONDS)
      }

  def read[V](fn: FdbReadApi[CFS] => CompletableFuture[V]): Task[V] =
    for
      cancelRef <- Ref.make(NOOP_CALLBACK)
      fib <- ZIO
        .fromCompletableFuture {
          dbContext.db
            .readAsync { tx =>
              val f = ops
                .read[V](new FdbReadApi[CFS](if (clientOptions.useSnapshotReads) tx.snapshot() else tx, dbContext), fn)
              Unsafe.unsafely {
                rt.unsafe.run(
                  cancelRef.set(() => f.cancel(true))
                )
              }
              f
            }
            .orTimeout(6, TimeUnit.SECONDS)
        }
        .fork
      ret <- fib.join.onInterrupt(cancelRef.get.map(_()) *> fib.interrupt)
    yield ret

  def write[V](name: => String, fn: FdbWriteApi[CFS] => CompletableFuture[V]): Task[V] =
    ZIO
      .acquireReleaseWith(ZIO.succeed(dbContext.db.createTransaction())) { tx =>
        ZIO.succeed(tx.close())
      } { tx =>
        TaskUtils
          .fromUninterruptibleCompletableFuture(
            name, {
              ops
                .write[V](
                  new FdbWriteApi[CFS](
                    tx,
                    dbContext,
                    clientOptions.disableWriteConflictChecking,
                    clientOptions.useSnapshotReads
                  ),
                  fn
                )
                .thenCompose(v => tx.commit().thenApply(_ => v))
                .whenComplete((_, _) => tx.close())
                .orTimeout(6, TimeUnit.SECONDS)
            }
          )
      }
      .retry(clientOptions.writeCustomRetrySchedule.getOrElse(defaultWriteRetrySchedule))

  def watchKey[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte])(using
    CFS <:< Col
  ): Task[CompletableFuture[Unit]] =
    write("watch", writeApi => CompletableFuture.completedFuture(writeApi.watch(column, key)))
  //   override def watchKeySource[Col <: CF](
  //     column: Col,
  //     key: Array[Byte]
  //   ): Source[Option[Array[Byte]], Future[Done]] = {
  //     val prefixedKey = dbContext.prefixKey(column, key)

  //     val watchTimeout = zio.duration.Duration.fromScala(clientOptions.watchTimeout)
  //     val watchMinLatency = clientOptions.watchMinLatency
  //     val watchMinLatencyNanos = watchMinLatency.toNanos

  //     Source
  //       .lazySource(() => {
  //         val zStream = zio.stream.Stream
  //           .repeatEffect {
  //             TaskUtils
  //               .fromCancellableCompletableFuture(
  //                 dbContext.db.runAsync { tx =>
  //                   tx.get(prefixedKey).thenApply { value =>
  //                     val maybeValue = Option(value)
  //                     val watchCompletableFuture = tx.watch(prefixedKey)
  //                     (watchCompletableFuture, maybeValue)
  //                   }
  //                 }: CompletableFuture[(CompletableFuture[Void], Option[Array[Byte]])]
  //               )
  //           }
  //           .flatMap { case (future, maybeValue) =>
  //             val watchTask = TaskUtils
  //               .fromCancellableCompletableFuture(future)
  // //              .log("watch task")
  //               .unit

  //             val watchTaskWithTimeout = watchTimeout match {
  //               case Duration.Infinity => watchTask
  //               case d => watchTask.timeoutTo(())(identity)(d)
  //             }

  //             val watchTaskWithRecovery = watchTaskWithTimeout
  //               .catchSome {
  //                 case e: FDBException if e.getCode == 1009 => // Request for future version
  //                   ZIO.unit
  //                 case e: FDBException =>
  //                   logger.warn(s"[watchKeySource][fdbErrorCode=${e.getCode}] ${e.getMessage}")
  //                   ZIO.unit
  //               }
  //               .timed
  //               .flatMap { case (elapsed, _) =>
  //                 val elapsedNanos = elapsed.toNanos
  //                 ZIO
  //                   .unit
  //                   .delay(java.time.Duration.ofNanos(watchMinLatencyNanos - elapsedNanos))
  //                   .when(elapsedNanos < watchMinLatencyNanos)
  //               }
  //               .as(Left(()))

  //             zio
  //               .stream
  //               .Stream(Right(maybeValue))
  //               .merge(zio.stream.Stream.fromEffect(
  //                 watchTaskWithRecovery
  //               ))
  //           }
  //           .collect { case Right(maybeValue) => maybeValue }
  //           .changes

  //         val promise = scala.concurrent.Promise[Done]()
  //         val publisher: Publisher[Option[Array[Byte]]] = subscriber => {
  //           if (subscriber == null) {
  //             throw new NullPointerException("Subscriber must not be null.")
  //           }
  //           else {
  //             rt.unsafeRunAsync_(
  //               for {
  //                 demand <- Queue.unbounded[Long]
  //                 _ <- UIO(subscriber.onSubscribe(createSubscription(subscriber, demand, rt)))
  //                 _ <- zStream
  //                   .run(demandUnfoldSink(subscriber, demand))
  //                   .catchAll(e => UIO(subscriber.onError(e)))
  //                   .onExit { exit: Exit[Throwable, Unit] =>
  //                     exit.foldM(
  //                       cause => UIO(promise.failure(cause.squash)),
  //                       _ => UIO(promise.success(Done))
  //                     )
  //                   }
  //                   .forkDaemon
  //               } yield ()
  //             )
  //           }
  //         }

  //         Source
  //           .fromPublisher(publisher)
  //           .mapMaterializedValue(_ => promise.future)
  //       })
  //       .mapMaterializedValue(future => future.flatten)
  //   }

  override def getTask[Col <: ColumnFamily[_, _]](
    column: Col,
    constraints: KvdbKeyConstraintList
  )(using CFS <:< Col): Task[Option[(Array[Byte], Array[Byte])]] =
    read(_.get(column, constraints.constraints))

  override def getRangeTask[Col <: ColumnFamily[_, _]](column: Col, range: KvdbKeyRange, reverse: Boolean)(using
    CFS <:< Col
  ): Task[List[KvdbPair]] =
    read(_.getRange(column, range, reverse))

  override def batchGetTask[Col <: ColumnFamily[_, _]](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  )(using CFS <:< Col): Task[Seq[Option[KvdbPair]]] =
    read { api =>
      val futures = requests.map { req =>
        api.get(column, req.constraints)
      }

      CompletableFuture
        .allOf(futures: _*)
        .thenApply(_ => futures.map(_.join()))
    }

  override def batchGetRangeTask[Col <: ColumnFamily[_, _]](
    column: Col,
    ranges: Seq[KvdbKeyRange],
    reverse: Boolean
  )(using CFS <:< Col): Task[List[List[KvdbPair]]] =
    if (!ranges.forall(_.limit > 0))
      ZIO.fail(
        InvalidKvdbArgumentException(s"ranges contains non-positive integers")
      )
    else
      read { api =>
        val futures = ranges.view
          .map { range => api.getRange(column, range, reverse) }
          .toList
        CompletableFuture
          .allOf(futures: _*)
          .thenApply(_ => futures.map(_.join()))
      }

  override def putTask[Col <: ColumnFamily[_, _]](
    column: Col,
    key: Array[Byte],
    value: Array[Byte]
  )(using CFS <:< Col): Task[Unit] =
    write(
      s"putTask column=${column.id}",
      api => {
        api.put(column, key, value)
        COMPLETED_FUTURE
      }
    )

  override def deleteTask[Col <: ColumnFamily[_, _]](column: Col, key: Array[Byte])(using CFS <:< Col): Task[Unit] =
    write(
      s"deleteTask column=${column.id}",
      api => {
        api.delete(column, key)
        COMPLETED_FUTURE
      }
    )

  override def deletePrefixTask[Col <: ColumnFamily[_, _]](column: Col, prefix: Array[Byte])(using
    CFS <:< Col
  ): Task[Long] =
    write(
      s"deletePrefixTask column=${column.id}",
      api => {
        api.deletePrefix(column, prefix)
        COMPLETED_FUTURE
      }
    ).as(0L)

  override def estimateCount[Col <: ColumnFamily[_, _]](column: Col)(using CFS <:< Col): Task[Long] =
    read(api =>
      api.tx.getEstimatedRangeSizeBytes(Range.startsWith(dbContext.columnPrefix(column))).thenApply(_.longValue())
    )

  final case class FdbStreamState(tx: Transaction, api: FdbReadApi[CFS], closeTx: () => Unit)

  override def iterateStream[Col <: ColumnFamily[_, _]](
    column: Col,
    range: KvdbKeyRange
  )(using CFS <:< Col): Stream[Throwable, KvdbBatch] =
    ZStream.suspend {
      val initialTx = dbContext.db.createTransaction()
      val initialApi = new FdbReadApi[CFS](initialTx, dbContext)
      val closeTx = () => initialTx.close()

      ZStream
        .fromZIO {
          ZIO.fromCompletableFuture {
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
                      new FdbReadApi[CFS](tx, dbContext),
                      _.tx.snapshot().getRange(startKeySelector, endKeySelector).iterator()
                    )
                    iterator -> closeTx
                  }

                  FdbIterateStream
                    .iterateStream(
                      initialRange = KvdbKeyRange(fromConstraints, toConstraints),
                      iterate = iterate,
                      keyValidator = keyValidator,
                      keyTransformer = keyTransformer,
                      shutdownSignal = dbContext.dbCloseSignal,
                      maxBatchBytes = clientOptions.batchReadMaxBatchBytes,
                      disableIsolationGuarantee = clientOptions.disableIsolationGuarantee
                    )

                case Right((k, _)) =>
                  closeTx()
                  val message =
                    s"Starting key: [${ByteArrayUtil.printable(k)}] satisfies fromConstraints ${fromConstraints.show} " +
                      s"but does not satisfy toConstraint: ${toConstraints.show}"
                  ZStream.fail(SeekFailure(message))

                case Left(k) =>
                  closeTx()
                  val message = {
                    if (k.nonEmpty) {
                      s"Starting key: [${ByteArrayUtil.printable(k)}] does not satisfy constraints: ${fromConstraints.show}"
                    }
                    else s"There's no starting key satisfying constraint: ${fromConstraints.show}"
                  }

                  ZStream.fail(SeekFailure(message))
              }
            }
          }
        }
        .flatten
    }

  override def transactionTask(
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
    write(
      "transactionTask",
      api => {
        api.transact(actions)
        COMPLETED_FUTURE
      }
    )
  }

  override def conditionalTransactionTask(
    reads: List[TransactionRead.Get],
    condition: List[Option[(Array[Byte], Array[Byte])]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
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
              api.transact(actions)
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

  override def tailSource[Col <: ColumnFamily[_, _]](
    column: Col,
    range: KvdbKeyRange
  )(using CFS <:< Col): Stream[Throwable, KvdbTailBatch] = {
    ZStream.suspend {
      if (range.from.isEmpty) {
        ZStream.fail(
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
          val api = new FdbReadApi[CFS](tx, dbContext)

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

        FdbTailStream
          .tailStream(
            initialRange = KvdbKeyRange(fromConstraints, toConstraints),
            iterate = iterate,
            keyValidator = keyValidator,
            keyTransformer = keyTransformer,
            shutdownSignal = dbContext.dbCloseSignal,
            maxBatchBytes = clientOptions.batchReadMaxBatchBytes,
            tailPollingInterval = clientOptions.tailPollingMaxInterval,
            tailPollingBackoffFactor = clientOptions.tailPollingBackoffFactor
          )
      }
    }
  }

  override def concurrentTailSource[Col <: ColumnFamily[_, _]](
    column: Col,
    ranges: List[KvdbKeyRange]
  )(using CFS <:< Col): ZStream[Any, Throwable, (Int, KvdbTailBatch)] =
    ZStream.suspend {
      if (ranges.exists(_.from.isEmpty)) {
        ZStream.fail(
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
            ZStream.fail(UnsupportedKvdbOperationException("ranges cannot be empty"))

          case head :: Nil =>
            tail(0, head)

          case _ =>
            ZStream
              .mergeAll(n = ranges.length)(
                ranges.zipWithIndex.map { case (r, i) => tail(i, r) }: _*
              )
        }
      }
    }

  override def dropColumnFamily[Col <: ColumnFamily[_, _]](column: Col)(using CFS <:< Col): Task[Unit] = {
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
