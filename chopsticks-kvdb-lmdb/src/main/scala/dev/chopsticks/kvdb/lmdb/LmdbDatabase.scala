package dev.chopsticks.kvdb.lmdb

import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Sink, Source}
import akka.{Done, NotUsed}
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.fp.zio_ext._
import dev.chopsticks.kvdb.KvdbDatabase.{keySatisfies, KvdbClientOptions}
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder._
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits._
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbException._
import dev.chopsticks.kvdb.util._
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import org.lmdbjava._
import pureconfig.ConfigConvert
import squants.information.Information
import zio.clock.Clock
import zio.internal.Executor
import zio.{Schedule, Task, ZIO, ZManaged}

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.{ControlThrowable, NonFatal}

object LmdbDatabase extends StrictLogging {
  type PutBatch = Seq[(Dbi[ByteBuffer], Array[Byte], Array[Byte])]

  final case class LmdbDatabaseConfig(
    path: NonEmptyString,
    maxSize: Information,
    noSync: Boolean,
    ioDispatcher: NonEmptyString,
    clientOptions: KvdbClientOptions = KvdbClientOptions()
  )

  object LmdbDatabaseConfig {
    import dev.chopsticks.util.config.PureconfigConverters._
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[LmdbDatabaseConfig]
  }

  final case class FatalError(message: String, cause: Throwable) extends ControlThrowable(message)

  final case class ReadTxnContext(env: Env[ByteBuffer], txn: Txn[ByteBuffer], cursor: Cursor[ByteBuffer])

  final case class LmdbContext[BaseCol <: ColumnFamily[_, _]](
    env: Env[ByteBuffer],
    dbiMap: Map[BaseCol, Dbi[ByteBuffer]],
    ioExecutor: Executor,
    ioDispatcher: String
  ) {
    val dbCloseSignal = new KvdbCloseSignal
    lazy val writeExecutor: ExecutorService = Executors.newSingleThreadExecutor()
    private val isClosed = new AtomicBoolean(false)

    def close(): ZIO[Clock, Throwable, Unit] = {
      import zio.duration._

      val task = for {
        _ <- Task(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
          Task.fail(ClosedException).unless(isClosed)
        }
        _ <- Task {
          writeExecutor.shutdown()
          writeExecutor.awaitTermination(10, TimeUnit.SECONDS)
        }.lock(ioExecutor)
        _ <- Task(dbCloseSignal.tryComplete(Failure(ClosedException)))
        _ <- Task(dbCloseSignal.hasNoListeners)
          .repeat(Schedule.fixed(100.millis).untilInput[Boolean](identity))
        _ <- Task {
          dbiMap.foreach(_._2.close())
          env.close()
        }.lock(ioExecutor)
      } yield ()

      task
    }

    def obtain(): ZIO[Any, Throwable, LmdbContext[BaseCol]] = {
      Task(isClosed.get)
        .flatMap { isClosed =>
          if (isClosed) Task.fail(ClosedException)
          else ZIO.succeed(this)
        }
    }

    def getKvdbi[Col <: BaseCol](column: Col): Dbi[ByteBuffer] = {
      dbiMap.getOrElse(
        column,
        throw InvalidKvdbArgumentException(
          s"Column family: $column doesn't exist in dbiMap: $dbiMap"
        )
      )
    }
  }

  private val ClosedException = KvdbAlreadyClosedException("Database was already closed")

  private def bufferToArray(b: ByteBuffer): Array[Byte] = {
    val value: Array[Byte] = new Array[Byte](b.remaining)
    val _ = b.get(value)
    value
  }

  private def putInBuffer(buffer: ByteBuffer, bytes: Array[Byte]): ByteBuffer = {
    {
      val _ = buffer.clear()
    }
    val _ = buffer.put(bytes).flip()
    buffer
  }

  def manage[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS],
    config: LmdbDatabaseConfig
  ): ZManaged[AkkaEnv with KvdbIoThreadPool with Clock, Throwable, KvdbDatabase[BCF, CFS]] = {
    KvdbMaterialization.validate(materialization) match {
      case Left(ex) => ZManaged.fail(ex)
      case Right(mat) =>
        for {
          ioExecutor <- ZManaged.access[KvdbIoThreadPool](_.get.executor)
          refs <- ZManaged
            .make {
              Task {
                import better.files.Dsl._
                import better.files._

                val file = File(config.path)
                val _ = mkdirs(file)
                val extraFlags = {
                  if (config.noSync) Vector(EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC)
                  else Vector.empty
                }

                val flags = Vector(EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD) ++ extraFlags
                val env = Env.create
                  .setMapSize(config.maxSize.toBytes.toLong)
                  .setMaxDbs(materialization.columnFamilySet.value.size)
                  .setMaxReaders(4096)
                  .open(file.toJava, flags: _*)

                val columnRefs: Map[BCF[_, _], Dbi[ByteBuffer]] = materialization.columnFamilySet.value.map { col =>
                  (col, env.openDbi(col.id, DbiFlags.MDB_CREATE))
                }.toMap

                LmdbContext[BCF[_, _]](env, columnRefs, ioExecutor, config.ioDispatcher)
              }.lock(ioExecutor)
            } { refs => refs.close().orDie }
          rt <- ZIO.runtime[AkkaEnv].toManaged_
        } yield {
          new LmdbDatabase[BCF, CFS](mat, config.clientOptions, refs)(rt)
        }
    }

  }
}

final class LmdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] private (
  val materialization: KvdbMaterialization[BCF, CFS],
  val clientOptions: KvdbClientOptions,
  dbContext: LmdbDatabase.LmdbContext[BCF[_, _]]
)(implicit
  rt: zio.Runtime[AkkaEnv]
) extends KvdbDatabase[BCF, CFS]
    with StrictLogging {
  import LmdbDatabase._

  type Refs = LmdbContext[CF]

  private lazy val writeEc = ExecutionContext.fromExecutor(dbContext.writeExecutor)
  private lazy val writeZioExecutor = Executor.fromExecutionContext(Int.MaxValue)(writeEc)
  private lazy val readZioExecutor = dbContext.ioExecutor

  val activeTxnCounter = new LongAdder
  val activeCursorCounter = new LongAdder

  def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): LmdbDatabase[BCF, CFS] = {
    val newOptions = modifier(clientOptions)
    new LmdbDatabase[BCF, CFS](materialization, newOptions, dbContext)
  }

  private def obtainContext: Task[Refs] = dbContext.obtain()

  private def createTxn(env: Env[ByteBuffer], forWrite: Boolean = false): Txn[ByteBuffer] = {
    val txn = if (forWrite) env.txnWrite() else env.txnRead()
    activeTxnCounter.increment()
    txn
  }

  private def closeTxn(txn: Txn[ByteBuffer]): Unit = {
    txn.close()
    activeTxnCounter.decrement()
  }

  private def openCursor(dbi: Dbi[ByteBuffer], txn: Txn[ByteBuffer]): Cursor[ByteBuffer] = {
    val cursor = dbi.openCursor(txn)
    activeCursorCounter.increment()
    cursor
  }

  private def closeCursor(cursor: Cursor[ByteBuffer]): Unit = {
    cursor.close()
    activeCursorCounter.decrement()
  }

  private def createTxnContext[Col <: CF](column: Col, refs: Refs): ReadTxnContext = {
    val env = refs.env

    try {
      val txn = createTxn(env)
      val cursor = openCursor(refs.getKvdbi(column), txn)
      ReadTxnContext(env, txn, cursor)
    }
    catch {
      case NonFatal(ex) =>
        throw FatalError(
          s"Fatal db error while trying to createTxnContext for column: ${column.id}: ${ex.toString}",
          ex
        )
    }
  }

  private def closeTxnContext(ctx: ReadTxnContext): Unit = {
    try {
      closeCursor(ctx.cursor)
      closeTxn(ctx.txn)
    }
    catch {
      case NonFatal(ex) => throw FatalError(s"Fatal db error while trying to closeTxnContext: ${ex.toString}", ex)
    }
  }

  private def satisfiyingPairAtCursor(
    isValid: Boolean,
    cursor: Cursor[ByteBuffer],
    constraints: List[KvdbKeyConstraint]
  ): Either[Array[Byte], KvdbPair] = {
    if (isValid) {
      val key = bufferToArray(cursor.key())
      if (keySatisfies(key, constraints)) Right((key, bufferToArray(cursor.`val`())))
      else Left(key)
    }
    else Left(Array.emptyByteArray)
  }

  private def doGet(
    cursor: Cursor[ByteBuffer],
    reuseableKeyBuffer: ByteBuffer,
    constraints: List[KvdbKeyConstraint]
  ): Either[Array[Byte], KvdbPair] = {
    if (constraints.isEmpty) {
      Left(Array.emptyByteArray)
    }
    else {
      val headConstraint = constraints.head
      val tailConstraints = constraints.tail

      val headOperand = headConstraint.operand.toByteArray
      val operator = headConstraint.operator

      operator match {
        case Operator.FIRST | Operator.LAST | Operator.EQUAL | Operator.GREATER_EQUAL =>
          val isValid = {
            if (operator.isFirst) cursor.first()
            else if (operator.isLast) cursor.last()
            else
              cursor.get(
                putInBuffer(reuseableKeyBuffer, headOperand),
                if (operator.isEqual) GetOp.MDB_SET_KEY else GetOp.MDB_SET_RANGE
              )
          }

          satisfiyingPairAtCursor(isValid, cursor, tailConstraints)

        case Operator.LESS_EQUAL | Operator.LESS =>
          if (cursor.get(putInBuffer(reuseableKeyBuffer, headOperand), GetOp.MDB_SET_RANGE)) {
            val key = bufferToArray(cursor.key())

            if (operator.isLess || !KeySerdes.isEqual(key, headOperand))
              satisfiyingPairAtCursor(cursor.prev(), cursor, tailConstraints)
            else if (keySatisfies(key, tailConstraints)) Right((key, bufferToArray(cursor.`val`())))
            else Left(key)
          }
          else Left(Array.emptyByteArray)

        case Operator.GREATER =>
          val isValid = cursor.get(putInBuffer(reuseableKeyBuffer, headOperand), GetOp.MDB_SET_RANGE)

          if (isValid) {
            val key = bufferToArray(cursor.key())
            //noinspection CorrespondsUnsorted
            if (KeySerdes.isEqual(key, headOperand)) satisfiyingPairAtCursor(cursor.next(), cursor, tailConstraints)
            else {
              if (keySatisfies(key, tailConstraints)) Right((key, bufferToArray(cursor.`val`())))
              else Left(key)
            }
          }
          else Left(Array.emptyByteArray)

        case Operator.PREFIX =>
          val isValid = cursor.get(putInBuffer(reuseableKeyBuffer, headOperand), GetOp.MDB_SET_RANGE)

          if (isValid) {
            val key = bufferToArray(cursor.key())
            if (KeySerdes.isPrefix(headOperand, key) && keySatisfies(key, tailConstraints))
              Right((key, bufferToArray(cursor.`val`())))
            else Left(key)
          }
          else Left(Array.emptyByteArray)

        case Operator.Unrecognized(v) =>
          throw new IllegalArgumentException(s"Got Operator.Unrecognized($v)")
      }
    }
  }

  private def writeTask[R](task: Task[R]): Task[R] = {
    task.lock(writeZioExecutor)
  }

  private def readTask[R](task: Task[R]): Task[R] = {
    task.lock(readZioExecutor)
  }

  private val reuseablePutKeyBuffer = allocateDirect(511)
  private var reuseablePutValueBufferSize = 1024
  private var reuseablePutValueBuffer = allocateDirect(reuseablePutValueBufferSize)

  private def doPut(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], key: Array[Byte], value: Array[Byte]): Boolean = {
    if (value.length > reuseablePutValueBufferSize) {
      reuseablePutValueBufferSize = value.length
      reuseablePutValueBuffer = allocateDirect(value.length)
    }
    dbi.put(txn, putInBuffer(reuseablePutKeyBuffer, key), putInBuffer(reuseablePutValueBuffer, value))
  }

  private def doDelete(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], key: Array[Byte]): Boolean = {
    dbi.delete(txn, putInBuffer(reuseablePutKeyBuffer, key))
  }

  private def doDeleteRange(
    txn: Txn[ByteBuffer],
    dbi: Dbi[ByteBuffer],
    fromKey: Array[Byte],
    toKey: Array[Byte]
  ): Long = {
    val cursor = openCursor(dbi, txn)

    try {
      if (
        cursor.get(
          putInBuffer(reuseablePutKeyBuffer, fromKey),
          GetOp.MDB_SET_RANGE
        )
      ) {
        val firstKey = bufferToArray(cursor.key())

        if (KeySerdes.isPrefix(fromKey, firstKey)) {
          val it = Iterator.single(firstKey) ++ Iterator
            .continually {
              cursor.next()
            }
            .takeWhile(identity)
            .map(_ => bufferToArray(cursor.key()))

          val count = it
            .takeWhile(k => KeySerdes.compare(k, toKey) < 0)
            .foldLeft(0L) { (count, key) =>
              if (dbi.delete(txn, putInBuffer(reuseablePutKeyBuffer, key))) count + 1
              else count
            }

          count
        }
        else {
          0L
        }
      }
      else 0L
    }
    finally closeCursor(cursor)
  }

  private def doDeletePrefix(txn: Txn[ByteBuffer], dbi: Dbi[ByteBuffer], prefix: Array[Byte]): Long = {
    val cursor = openCursor(dbi, txn)

    try {
      if (cursor.get(putInBuffer(reuseablePutKeyBuffer, prefix), GetOp.MDB_SET_RANGE)) {
        val it = Iterator.single(bufferToArray(cursor.key())) ++ Iterator
          .continually {
            cursor.next()
          }
          .takeWhile(identity)
          .map(_ => bufferToArray(cursor.key()))

        val count = it
          .takeWhile(KeySerdes.isPrefix(prefix, _))
          .foldLeft(0L) { (count, key) =>
            if (dbi.delete(txn, putInBuffer(reuseablePutKeyBuffer, key))) count + 1
            else count
          }

        count
      }
      else 0L
    }
    finally closeCursor(cursor)
  }

  private val emptyLabels = Map.empty[String, String]

  override def statsTask: Task[Map[(String, Map[String, String]), Double]] = {
    readTask(obtainContext.map { refs =>
      val info = refs.env.info()

      val txn = createTxn(refs.env)
      val statsMap =
        try {
          val allStats = refs.dbiMap.values.map(_.stat(txn))

          Map(
            ("lmdb_branch_pages", emptyLabels) -> allStats.map(_.branchPages.toDouble).sum,
            ("lmdb_depth", emptyLabels) -> allStats.map(_.depth.toDouble).sum,
            ("lmdb_leaf_pages", emptyLabels) -> allStats.map(_.leafPages.toDouble).sum,
            ("lmdb_overflow_pages", emptyLabels) -> allStats.map(_.overflowPages.toDouble).sum,
            ("lmdb_page_size", emptyLabels) -> allStats.map(_.pageSize.toDouble).sum,
            ("lmdb_entries", emptyLabels) -> allStats.map(_.entries.toDouble).sum
          )
        }
        finally closeTxn(txn)

      Map(
        ("lmdb_last_page_number", emptyLabels) -> info.lastPageNumber.toDouble,
        ("lmdb_last_transaction_id", emptyLabels) -> info.lastTransactionId.toDouble,
        ("lmdb_map_address", emptyLabels) -> info.mapAddress.toDouble,
        ("lmdb_map_size", emptyLabels) -> info.mapSize.toDouble,
        ("lmdb_max_readers", emptyLabels) -> info.maxReaders.toDouble,
        ("lmdb_num_readers", emptyLabels) -> info.numReaders.toDouble,
        ("lmdb_active_cursors", emptyLabels) -> activeCursorCounter.doubleValue(),
        ("lmdb_active_txns", emptyLabels) -> activeTxnCounter.doubleValue()
      ) ++ statsMap
    })
  }

  override def getTask[Col <: CF](column: Col, constraints: KvdbKeyConstraintList): Task[Option[KvdbPair]] = {
    readTask(obtainContext.map { refs =>
      val txn = createTxn(refs.env)

      try {
        val dbi = refs.getKvdbi(column)
        val cursor = openCursor(dbi, txn)

        try {
          val reuseableBuffer = allocateDirect(refs.env.getMaxKeySize)
          doGet(cursor, reuseableBuffer, constraints.constraints).toOption
        }
        finally closeCursor(cursor)
      }
      finally closeTxn(txn)
    })
  }

  override def getRangeTask[Col <: CF](column: Col, range: KvdbKeyRange): Task[List[KvdbPair]] = {
    if (range.limit < 1) {
      Task.fail(
        InvalidKvdbArgumentException(s"range.limit of '${range.limit}' is invalid, must be a positive integer")
      )
    }
    else {
      // TODO: Optimize with a better implementation tailored to range scanning with a known limit
      Task.fromFuture { _ =>
        val akkaService = rt.environment.get
        import akkaService.actorSystem

        iterateSource(column, range)
          .recoverWithRetries(
            1,
            {
              case _: SeekFailure => Source.empty
            }
          )
          .mapConcat(_.toList)
          .take(range.limit.toLong)
          .runWith(Sink.collection[KvdbPair, List[KvdbPair]])
      }
    }
  }

  override def batchGetTask[Col <: CF](
    column: Col,
    requests: Seq[KvdbKeyConstraintList]
  ): Task[Seq[Option[KvdbPair]]] = {
    readTask(obtainContext.map { refs =>
      val txn = createTxn(refs.env)

      try {
        val dbi = refs.getKvdbi(column)
        val cursor = openCursor(dbi, txn)
        try {
          val reuseableBuffer = allocateDirect(refs.env.getMaxKeySize)
          requests.map(r => doGet(cursor, reuseableBuffer, r.constraints).toOption)
        }
        finally closeCursor(cursor)
      }
      finally closeTxn(txn)
    })
  }

  override def batchGetRangeTask[Col <: CF](column: Col, ranges: Seq[KvdbKeyRange]): Task[Seq[List[KvdbPair]]] = {
    ZIO
      .foreachPar(ranges) { range =>
        getRangeTask(column, range)
      }
  }

  override def estimateCount[Col <: CF](column: Col): Task[Long] = {
    readTask(obtainContext.map { refs =>
      val txn = createTxn(refs.env)
      try {
        refs.getKvdbi(column).stat(txn).entries
      }
      finally closeTxn(txn)
    })
  }

  override def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    writeTask(
      obtainContext
        .map { refs =>
          val txn = createTxn(refs.env, forWrite = true)
          try {
            assert(doPut(txn, refs.getKvdbi(column), key, value))
            txn.commit()
          }
          finally closeTxn(txn)
        }
    )
  }

  override def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit] = {
    writeTask(for {
      refs <- obtainContext
      _ <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getKvdbi(column)
          val _ = doDelete(txn, dbi, key)
          txn.commit()
        }
        finally closeTxn(txn)
      }
    } yield ())
  }

  override def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long] = {
    writeTask(for {
      refs <- obtainContext
      count <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getKvdbi(column)
          val deletedCount = doDeletePrefix(txn, dbi, prefix)
          txn.commit()
          deletedCount
        }
        finally closeTxn(txn)
      }
    } yield count)
  }

  override def watchKeySource[Col <: CF](column: Col, key: Array[Byte]): Source[Option[Array[Byte]], Future[Done]] = ???

  override def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = obtainContext
          .map {
            refs =>
              val init = () => {
                val ctx = createTxnContext(column, refs)
                val ReadTxnContext(env, _, cursor) = ctx
                val close = () => closeTxnContext(ctx)
                val fromConstraints = range.from
                val toConstraints = range.to

                doGet(cursor, allocateDirect(env.getMaxKeySize), fromConstraints) match {
                  case Right(startingPair) if keySatisfies(startingPair._1, toConstraints) =>
                    // scalastyle:off null
                    val it = Iterator(startingPair) ++ Iterator
                      .continually {
                        if (cursor.next()) bufferToArray(cursor.key()) else null
                      }
                      .takeWhile(key => key != null && keySatisfies(key, toConstraints))
                      .map(key => (key, bufferToArray(cursor.`val`())))
                    // scalastyle:on null

                    Right(KvdbIterateSourceGraph.Refs(it, close))

                  case Right((k, _)) =>
                    close()
                    val message = column
                      .deserializeKey(k)
                      .map { d =>
                        s"Starting key: [$d] satisfies fromConstraints [${fromConstraints.show}] but does not satisfy toConstraint: [${toConstraints.show}]"
                      }
                      .getOrElse(
                        s"Failed decoding key with bytes: ${k.mkString(",")}. Constraints: [${fromConstraints.show}]"
                      )

                    Left(SeekFailure(message))

                  case Left(k) =>
                    close()
                    val message = {
                      if (k.nonEmpty) {
                        column
                          .deserializeKey(k)
                          .map { d => s"Starting key: [$d] does not satisfy constraints: [${fromConstraints.show}]" }
                          .getOrElse(
                            s"Failed decoding key with bytes: [${k.mkString(",")}]. Constraints: [${fromConstraints.show}]"
                          )
                      }
                      else s"There's no starting key satisfying constraint: [${fromConstraints.show}]"
                    }

                    Left(SeekFailure(message))
                }
              }

              Source
                .fromGraph(
                  new KvdbIterateSourceGraph(
                    init,
                    dbContext.dbCloseSignal,
                    dbContext.ioDispatcher,
                    clientOptions.batchReadMaxBatchBytes
                  )
                )
          }

        task.unsafeRunToFuture
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def concurrentTailSource[Col <: CF](
    column: Col,
    ranges: List[KvdbKeyRange]
  ): Source[KvdbIndexedTailBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = obtainContext
          .map {
            refs =>
              if (ranges.exists(_.from.isEmpty)) {
                Source.failed(
                  UnsupportedKvdbOperationException(
                    "range.from cannot be empty for tailSource, since it can never be satisfied"
                  )
                )
              }
              else {
                def tail(index: Int, range: KvdbKeyRange) = {
                  createTailSource(refs, column, range)
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
          }

        task.unsafeRunToFuture
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  private def createTailSource[Col <: CF](
    refs: Refs,
    column: Col,
    range: KvdbKeyRange
  ): Source[KvdbTailBatch, NotUsed] = {
    val fromConstraints = range.from
    val toConstraints = range.to

    val init = () => {
      val ctx = createTxnContext(column, refs)
      val ReadTxnContext(env, txn, cursor) = ctx
      val close = () => closeTxnContext(ctx)
      val clear = () => {
        txn.reset()
        txn.renew()
        cursor.renew(txn)
      }

      // scalastyle:off null
      var lastKey: Array[Byte] = null
      // scalastyle:on null
      val lastKeyBuffer = allocateDirect(env.getMaxKeySize)

      val createIterator = () => {
        val headConstraints = {
          if (lastKey == null) fromConstraints
          else KvdbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey)) :: toConstraints
        }

        doGet(cursor, lastKeyBuffer, headConstraints) match {
          case Right(v) if keySatisfies(v._1, toConstraints) =>
            val tail = Iterator
              .continually {
                // scalastyle:off null
                if (cursor.next()) bufferToArray(cursor.key()) else null
                // scalastyle:on null
              }
              .takeWhile(key => key != null && keySatisfies(key, toConstraints))
              .map(key => (key, bufferToArray(cursor.`val`())))

            (Iterator.single(v) ++ tail).map { p =>
              lastKey = p._1 // Side-effecting
              p
            }

          case _ => Iterator.empty
        }
      }

      KvdbTailSourceGraph.Refs(createIterator, clear = clear, close = close)
    }

    Source
      .fromGraph(
        new KvdbTailSourceGraph(
          init,
          refs.dbCloseSignal,
          dbContext.ioDispatcher,
          clientOptions.batchReadMaxBatchBytes,
          clientOptions.tailPollingMaxInterval,
          clientOptions.tailPollingBackoffFactor
        )
      )
  }

  override def tailSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbTailBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = obtainContext
          .map { refs =>
            if (range.from.isEmpty) {
              Source.failed(
                UnsupportedKvdbOperationException(
                  "range.from cannot be empty for tailSource, since it can never be satisfied"
                )
              )
            }
            else {
              createTailSource(refs, column, range)
            }
          }

        task.unsafeRunToFuture
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  private def doTransaction(refs: Refs, txn: Txn[ByteBuffer], actions: Seq[TransactionWrite]): Unit = {
    actions.foreach {
      case TransactionPut(columnId, key, value) =>
        val _ = doPut(txn, refs.getKvdbi(columnFamilyWithId(columnId).get), key, value)

      case TransactionDelete(columnId, key) =>
        val _ = doDelete(txn, refs.getKvdbi(columnFamilyWithId(columnId).get), key)

      case TransactionDeleteRange(columnId, fromKey, toKey) =>
        val _ =
          doDeleteRange(txn, refs.getKvdbi(columnFamilyWithId(columnId).get), fromKey, toKey)
      case _: TransactionMutateAdd => ???
      case _: TransactionMutateMin => ???
      case _: TransactionMutateMax => ???
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
  override def transactionTask(actions: Seq[TransactionWrite]): Task[Unit] = {
    writeTask(for {
      refs <- obtainContext
      _ <- Task {
        val txn: Txn[ByteBuffer] = createTxn(refs.env, forWrite = true)

        try {
          doTransaction(refs, txn, actions)
          txn.commit()
        }
        finally closeTxn(txn)
      }
    } yield ())
  }

  override def conditionalTransactionTask(
    reads: List[TransactionGet],
    condition: List[Option[(Array[Byte], Array[Byte])]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
    writeTask(obtainContext.flatMap { refs =>
      val txn: Txn[ByteBuffer] = createTxn(refs.env, forWrite = true)

      try {
        val reuseableBuffer = allocateDirect(refs.env.getMaxKeySize)

        val pairs = for (read <- reads) yield {
          val dbi = refs.getKvdbi(columnFamilyWithId(read.columnId).get)
          val cursor = openCursor(dbi, txn)

          try {
            doGet(
              cursor,
              reuseableBuffer,
              List(KvdbKeyConstraint(operator = Operator.EQUAL, operand = ProtoByteString.copyFrom(read.key)))
            ).toOption
          }
          finally closeCursor(cursor)
        }

        val okToWrite = condition(pairs)

        if (okToWrite) {
          doTransaction(refs, txn, actions)
          txn.commit()
          Task.succeed(())
        }
        else {
          Task.fail(ConditionalTransactionFailedException("Condition returns false"))
        }
      }
      finally closeTxn(txn)
    })
  }

  override def dropColumnFamily[Col <: CF](column: Col): Task[Unit] = {
    writeTask(for {
      refs <- obtainContext
      _ <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getKvdbi(column)
          logger.info(s"Dropping column family: ${column.id}")
          dbi.drop(txn)
          txn.commit()
        }
        finally closeTxn(txn)
      }
    } yield ())
  }
}
