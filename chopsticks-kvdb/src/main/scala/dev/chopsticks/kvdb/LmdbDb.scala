package dev.chopsticks.kvdb
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Source}
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.codec.DbKey
import dev.chopsticks.kvdb.codec.DbKeyConstraints.Implicits._
import dev.chopsticks.kvdb.DbInterface.{keySatisfies, DbDefinition}
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.proto.db.DbKeyConstraint.Operator
import dev.chopsticks.proto.db._
import dev.chopsticks.kvdb.util.DbUtils._
import org.lmdbjava._
import scalaz.zio.blocking._
import scalaz.zio.clock.Clock
import scalaz.zio.internal.Executor
import scalaz.zio.{Task, TaskR, ZSchedule}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.{ControlThrowable, NonFatal}

object LmdbDb extends StrictLogging {
  type PutBatch = Seq[(Dbi[ByteBuffer], Array[Byte], Array[Byte])]

  final case class FatalDbError(message: String, cause: Throwable) extends Error(message, cause) with ControlThrowable

  final case class ReadTxnContext(env: Env[ByteBuffer], txn: Txn[ByteBuffer], cursor: Cursor[ByteBuffer])

  final case class DbReferences[DbDef <: DbDefinition](
    env: Env[ByteBuffer],
    dbiMap: Map[DbDef#BaseCol[_, _], Dbi[ByteBuffer]]
  ) {
    def getDbi[Col <: DbDef#BaseCol[_, _]](column: Col): Dbi[ByteBuffer] = {
      dbiMap.getOrElse(
        column,
        throw InvalidDbArgumentException(
          s"Column family: $column doesn't exist in dbiMap: $dbiMap"
        )
      )
    }
  }

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

//  private val monixReporter = UncaughtExceptionReporter {
//    case _: RejectedExecutionException => logger.warn("[LmdbDb] Operation rejected since database has already been closed")
//    case e => logger.error(e.getMessage, e)
//  }

  private val DbClosedException = DbAlreadyClosedException("Database was already closed")

  def apply[DbDef <: DbDefinition](
    definition: DbDef,
    path: String,
    maxSize: Long,
    noSync: Boolean,
    ioDispatcher: String
  )(
    implicit akkaEnv: AkkaEnv
  ): LmdbDb[DbDef] = new LmdbDb(definition, path, maxSize, noSync, ioDispatcher)
}

final class LmdbDb[DbDef <: DbDefinition](
  val definition: DbDef,
  path: String,
  maxSize: Long,
  noSync: Boolean,
  ioDispatcher: String
)(
  implicit akkaEnv: AkkaEnv
) extends DbInterface[DbDef]
    with StrictLogging {
  import LmdbDb._

  type Refs = DbReferences[DbDef]

  private lazy val writeExecutor: ExecutorService = Executors.newSingleThreadExecutor()
  private lazy val writeEc = ExecutionContext.fromExecutor(writeExecutor)
  private lazy val writeZioExecutor = Executor.fromExecutionContext(Int.MaxValue)(writeEc)
  private lazy val readEc = akkaEnv.actorSystem.dispatchers.lookup(ioDispatcher)
  private lazy val readZioExecutor = Executor.fromExecutionContext(Int.MaxValue)(readEc)

  val activeTxnCounter = new LongAdder
  val activeCursorCounter = new LongAdder

  private val dbCloseSignal = new DbCloseSignal

  private lazy val _references: Future[Refs] = Future {
    import better.files.Dsl._
    import better.files._

    val file = File(path)
    val _ = mkdirs(file)
    val extraFlags =
      if (noSync) Vector(EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC)
      else Vector.empty

    val flags = Vector(EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD) ++ extraFlags
    val env = Env.create
      .setMapSize(maxSize)
      .setMaxDbs(definition.columns.values.size)
      .setMaxReaders(4096)
      .open(file.toJava, flags: _*)

    val columns: Map[DbDef#BaseCol[_, _], Dbi[ByteBuffer]] = definition.columns.values.map { c =>
      val col = c.asInstanceOf[DbDef#BaseCol[_, _]]
      (col, env.openDbi(col.entryName, DbiFlags.MDB_CREATE))
    }.toMap

    DbReferences[DbDef](env, columns)
  }(writeEc)

  private val isClosed = new AtomicBoolean(false)

  private def references: Task[Refs] = Task(isClosed.get).flatMap { isClosed =>
    if (isClosed) Task.fail(DbClosedException)
    else {
      _references.value.fold(Task.fromFuture(_ => _references))(Task.fromTry(_))
    }
  }

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

  private def createTxnContext[Col <: DbDef#BaseCol[_, _]](column: Col, refs: Refs): ReadTxnContext = {
    val env = refs.env

    try {
      val txn = createTxn(env)
      val cursor = openCursor(refs.getDbi(column), txn)
      ReadTxnContext(env, txn, cursor)
    } catch {
      case NonFatal(ex) =>
        throw FatalDbError(
          s"Fatal db error while trying to createTxnContext for column: ${column.id}: ${ex.toString}",
          ex
        )
    }
  }

  private def closeTxnContext(ctx: ReadTxnContext): Unit = {
    try {
      closeCursor(ctx.cursor)
      closeTxn(ctx.txn)
    } catch {
      case NonFatal(ex) => throw FatalDbError(s"Fatal db error while trying to closeTxnContext: ${ex.toString}", ex)
    }
  }

  private def satisfiyingPairAtCursor(
    isValid: Boolean,
    cursor: Cursor[ByteBuffer],
    constraints: List[DbKeyConstraint]
  ): Either[Array[Byte], DbPair] = {
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
    constraints: List[DbKeyConstraint]
  ): Either[Array[Byte], DbPair] = {
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

            if (operator.isLess || !DbKey.isEqual(key, headOperand))
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
            if (DbKey.isEqual(key, headOperand)) satisfiyingPairAtCursor(cursor.next(), cursor, tailConstraints)
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
            if (DbKey.isPrefix(headOperand, key) && keySatisfies(key, tailConstraints))
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
          .takeWhile(DbKey.isPrefix(prefix, _))
          .foldLeft(0L) { (count, key) =>
            if (dbi.delete(txn, putInBuffer(reuseablePutKeyBuffer, key))) count + 1
            else count
          }

        count
      }
      else 0L
    } finally closeCursor(cursor)
  }

  val isLocal: Boolean = true

  def statsTask: Task[Map[String, Double]] = {
    readTask(references.map { refs =>
      val info = refs.env.info()

      val txn = createTxn(refs.env)
      val statsMap = try {
        val allStats = refs.dbiMap.values.map(_.stat(txn))

        Map(
          "lmdb_branch_pages" -> allStats.map(_.branchPages.toDouble).sum,
          "lmdb_depth" -> allStats.map(_.depth.toDouble).sum,
          "lmdb_leaf_pages" -> allStats.map(_.leafPages.toDouble).sum,
          "lmdb_overflow_pages" -> allStats.map(_.overflowPages.toDouble).sum,
          "lmdb_page_size" -> allStats.map(_.pageSize.toDouble).sum,
          "lmdb_entries" -> allStats.map(_.entries.toDouble).sum
        )
      } finally closeTxn(txn)

      Map(
        "lmdb_last_page_number" -> info.lastPageNumber.toDouble,
        "lmdb_last_transaction_id" -> info.lastTransactionId.toDouble,
        "lmdb_map_address" -> info.mapAddress.toDouble,
        "lmdb_map_size" -> info.mapSize.toDouble,
        "lmdb_max_readers" -> info.maxReaders.toDouble,
        "lmdb_num_readers" -> info.numReaders.toDouble,
        "lmdb_active_cursors" -> activeCursorCounter.doubleValue(),
        "lmdb_active_txns" -> activeTxnCounter.doubleValue()
      ) ++ statsMap
    })
  }

  def openTask(): Task[Unit] = references.map(_ => ())

  def getTask[Col <: DbDef#BaseCol[_, _]](column: Col, constraints: DbKeyConstraintList): Task[Option[DbPair]] = {
    readTask(references.map { refs =>
      val txn = createTxn(refs.env)

      try {
        val dbi = refs.getDbi(column)
        val cursor = openCursor(dbi, txn)

        try {
          val reuseableBuffer = allocateDirect(refs.env.getMaxKeySize)
          doGet(cursor, reuseableBuffer, constraints.constraints).toOption
        } finally closeCursor(cursor)
      } finally closeTxn(txn)
    })
  }

  def batchGetTask[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    requests: Seq[DbKeyConstraintList]
  ): Task[Seq[Option[DbPair]]] = {
    readTask(references.map { refs =>
      val txn = createTxn(refs.env)

      try {
        val dbi = refs.getDbi(column)
        val cursor = openCursor(dbi, txn)
        try {
          val reuseableBuffer = allocateDirect(refs.env.getMaxKeySize)
          requests.map(r => doGet(cursor, reuseableBuffer, r.constraints).toOption)
        } finally closeCursor(cursor)
      } finally closeTxn(txn)
    })
  }

  def estimateCount[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Long] = {
    readTask(references.map { refs =>
      val txn = createTxn(refs.env)
      try {
        refs.getDbi(column).stat(txn).entries
      } finally closeTxn(txn)
    })
  }

  def putTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    writeTask(
      references
        .map { refs =>
          val txn = createTxn(refs.env, forWrite = true)
          try {
            assert(doPut(txn, refs.getDbi(column), key, value))
            txn.commit()
          } finally closeTxn(txn)
        }
    )
  }

  def deleteTask[Col <: DbDef#BaseCol[_, _]](column: Col, key: Array[Byte]): Task[Unit] = {
    writeTask(for {
      refs <- references
      _ <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getDbi(column)
          val _ = doDelete(txn, dbi, key)
          txn.commit()
        } finally closeTxn(txn)
      }
    } yield ())
  }

  def deletePrefixTask[Col <: DbDef#BaseCol[_, _]](column: Col, prefix: Array[Byte]): Task[Long] = {
    writeTask(for {
      refs <- references
      count <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getDbi(column)
          val deletedCount = doDeletePrefix(txn, dbi, prefix)
          txn.commit()
          deletedCount
        } finally closeTxn(txn)
      }
    } yield count)
  }

  def iterateSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbBatch, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {
        val task = references
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

                    Right(DbIterateSourceGraphRefs(it, close))

                  case Right((k, _)) =>
                    close()
                    val message = column
                      .decodeKey(k)
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
                          .decodeKey(k)
                          .map { d =>
                            s"Starting key: [$d] does not satisfy constraints: [${fromConstraints.show}]"
                          }
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
                .fromGraph(new DbIterateSourceGraph(init, dbCloseSignal, ioDispatcher))

          }

        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  def iterateValuesSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbValueBatch, Future[NotUsed]] = {
    iterateSource(column, range)
      .map(_.map(_._2))
  }

  def batchTailSource[Col <: DbDef#BaseCol[_, _]](
    column: Col,
    ranges: List[DbKeyRange]
  )(
    implicit clientOptions: DbClientOptions
  ): Source[DbIndexedTailBatch, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {
        val task = references
          .map {
            refs =>
              if (ranges.exists(_.from.isEmpty)) {
                Source.failed(
                  UnsupportedDbOperationException(
                    "range.from cannot be empty for tailSource, since it can never be satisfied"
                  )
                )
              }
              else {
                def tail(index: Int, range: DbKeyRange) = {
                  createTailSource(refs, column, range)
                    .map(b => (index, b))
                }

                ranges match {
                  case Nil =>
                    Source.failed(UnsupportedDbOperationException("ranges cannot be empty"))

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

        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  private def createTailSource[Col <: DbDef#BaseCol[_, _]](
    refs: Refs,
    column: Col,
    range: DbKeyRange
  )(implicit clientOptions: DbClientOptions): Source[DbTailBatch, NotUsed] = {
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
          else DbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey)) :: toConstraints
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

      DbTailSourceGraphRefs(createIterator, clear = clear, close = close)
    }

    Source
      .fromGraph(new DbTailSourceGraph(init, dbCloseSignal, ioDispatcher))
  }

  def tailSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbTailBatch, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {
        val task = references
          .map { refs =>
            if (range.from.isEmpty) {
              Source.failed(
                UnsupportedDbOperationException(
                  "range.from cannot be empty for tailSource, since it can never be satisfied"
                )
              )
            }
            else {
              createTailSource(refs, column, range)
            }
          }

        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  def tailValuesSource[Col <: DbDef#BaseCol[_, _]](column: Col, range: DbKeyRange)(
    implicit clientOptions: DbClientOptions
  ): Source[DbTailValueBatch, Future[NotUsed]] = {
    tailSource(column, range)
      .map(_.map(_.map(_._2)))
  }

  @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
  def transactionTask(actions: Seq[DbTransactionAction]): Task[Unit] = {
    writeTask(for {
      refs <- references
      _ <- Task {
        val txn: Txn[ByteBuffer] = createTxn(refs.env, forWrite = true)

        try {
          for (action <- actions) {
            action.action match {
              case DbTransactionAction.Action.Put(DbPutRequest(columnId, key, value)) =>
                doPut(txn, refs.getDbi(columnFamilyWithId(columnId).get), key.toByteArray, value.toByteArray)
              case DbTransactionAction.Action.Delete(DbDeleteRequest(columnId, key)) =>
                doDelete(txn, refs.getDbi(columnFamilyWithId(columnId).get), key.toByteArray)
              case DbTransactionAction.Action.DeletePrefix(DbDeletePrefixRequest(columnId, prefix)) =>
                doDeletePrefix(txn, refs.getDbi(columnFamilyWithId(columnId).get), prefix.toByteArray)
              case _ =>
                throw new IllegalArgumentException(s"Invalid transaction action: ${action.action}")
            }
          }
          txn.commit()
        } finally closeTxn(txn)
      }
    } yield ())
  }

  def closeTask(): TaskR[Blocking with Clock, Unit] = {
    import scalaz.zio.duration._

    val task = for {
      refs <- references
      _ <- Task(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
        if (isClosed) Task.unit
        else Task.fail(DbClosedException)
      }
      _ <- blocking(Task {
        writeExecutor.shutdown()
        writeExecutor.awaitTermination(10, TimeUnit.SECONDS)
      })
      _ <- Task(dbCloseSignal.tryComplete(Failure(DbClosedException)))
      _ <- Task(dbCloseSignal.hasNoListeners)
        .repeat(ZSchedule.fixed(100.millis).untilInput[Boolean](identity))
      _ <- blocking(Task {
        refs.dbiMap.foreach(_._2.close())
        refs.env.close()
      })
    } yield ()

    task
  }

  def compactTask(): Task[Unit] = ???
  def startBulkInsertsTask(): Task[Unit] = ???
  def endBulkInsertsTask(): Task[Unit] = ???

  def dropColumnFamily[Col <: DbDef#BaseCol[_, _]](column: Col): Task[Unit] = {
    writeTask(for {
      refs <- references
      _ <- Task {
        val txn = createTxn(refs.env, forWrite = true)

        try {
          val dbi = refs.getDbi(column)
          logger.info(s"Dropping column family: ${column.id}")
          dbi.drop(txn)
          txn.commit()
        } finally closeTxn(txn)
      }
    } yield ())
  }
}
