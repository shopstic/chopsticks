package dev.chopsticks.kvdb.rocksdb

import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Sink, Source}
import akka.{Done, NotUsed}
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.akka_env.AkkaEnv
import dev.chopsticks.kvdb.KvdbDatabase.{keySatisfies, KvdbClientOptions}
import dev.chopsticks.kvdb.KvdbReadTransactionBuilder.TransactionGet
import dev.chopsticks.kvdb.KvdbWriteTransactionBuilder._
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits._
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabaseManager.RocksdbContext
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbException._
import dev.chopsticks.kvdb.util._
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import eu.timepit.refined.types.string.NonEmptyString
import org.rocksdb._
import pureconfig.ConfigConvert
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Task, ZIO, ZManaged}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Future
import scala.util.control.NonFatal

object RocksdbDatabase extends StrictLogging {
  final val DEFAULT_COLUMN_NAME: String = new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8)

  private val ESTIMATE_NUM_KEYS = "rocksdb.estimate-num-keys"

  final case class RocksdbDatabaseConfig(
    path: NonEmptyString,
    startWithBulkInserts: Boolean,
    useDirectIo: Boolean,
    ioDispatcher: NonEmptyString,
    readOnly: Boolean = false,
    clientOptions: KvdbClientOptions = KvdbClientOptions()
  )

  object RocksdbDatabaseConfig {
    import dev.chopsticks.util.config.PureconfigConverters._
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[RocksdbDatabaseConfig]
  }

  def manage[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
    config: RocksdbDatabaseConfig
  ): ZManaged[AkkaEnv with KvdbIoThreadPool with Blocking with Clock, Throwable, KvdbDatabase[BCF, CFS]] = {
    RocksdbMaterialization.validate(materialization) match {
      case Left(ex) => ZManaged.fail(ex)
      case Right(_) =>
        for {
          ioExecutor <- ZManaged.access[KvdbIoThreadPool](_.get.executor)
          context <- RocksdbDatabaseManager[BCF, CFS](materialization, ioExecutor, config).managedContext
          rt <- ZIO.runtime[AkkaEnv].toManaged_
        } yield {
          new RocksdbDatabase[BCF, CFS](materialization, config.clientOptions, context)(rt)
        }
    }
  }
}

final class RocksdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] private (
  val materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
  val clientOptions: KvdbClientOptions,
  dbContext: RocksdbContext[BCF[_, _]]
)(implicit rt: zio.Runtime[AkkaEnv])
    extends KvdbDatabase[BCF, CFS]
    with StrictLogging {
  import RocksdbDatabase._

  val isLocal: Boolean = true

  override def withOptions(modifier: KvdbClientOptions => KvdbClientOptions): KvdbDatabase[BCF, CFS] = {
    val newOptions = modifier(clientOptions)
    new RocksdbDatabase[BCF, CFS](materialization, newOptions, dbContext)
  }

  private def getColumnFamilyName(cf: CF): String = {
    if (cf == materialization.defaultColumnFamily) DEFAULT_COLUMN_NAME else cf.id
  }

  private val columnOptions: Map[CF, ColumnFamilyOptions] = materialization.columnFamilyConfigMap.map

  private def newReadOptions(): ReadOptions = {
    new ReadOptions()
  }

  private def newWriteOptions(): WriteOptions = {
    val o = new WriteOptions()
    if (clientOptions.forceSync) o.setSync(true) else o
  }

  private val dbCloseSignal = new KvdbCloseSignal

  private def ioTask[T](task: Task[T]): Task[T] = {
    task.lock(dbContext.ioExecutor)
  }

  def references: Task[RocksdbContext[CF]] = dbContext.obtain()

  def compactTask(): Task[Unit] = references.flatMap { refs =>
    ioTask(Task {
      refs.columnHandleMap.values.foreach { col => refs.db.compactRange(col) }
    })
  }

  private val cfMetrics = List(
    "num-immutable-mem-table",
    "num-immutable-mem-table-flushed",
    "mem-table-flush-pending",
    "num-running-flushes",
    "compaction-pending",
    "num-running-compactions",
    "background-errors",
    "oldest-snapshot-time",
    "num-snapshots",
    "num-live-versions",
    "min-log-number-to-keep",
    "min-obsolete-sst-number-to-keep",
    "cur-size-active-mem-table",
    "cur-size-all-mem-tables",
    "size-all-mem-tables",
    "estimate-table-readers-mem",
    "estimate-table-readers-mem",
    "estimate-live-data-size",
    "num-entries-active-mem-table",
    "num-entries-imm-mem-tables",
    "num-deletes-active-mem-table",
    "num-deletes-imm-mem-tables",
    "estimate-num-keys",
    "estimate-pending-compaction-bytes",
    "actual-delayed-write-rate",
    "is-write-stopped",
    "block-cache-capacity",
    "block-cache-usage",
    "block-cache-pinned-usage"
  ).map(n => (n, "rocksdb_cf_" + n.replace("-", "_")))

  override def statsTask: Task[Map[(String, Map[String, String]), Double]] = references.map { refs =>
    val tickers = TickerType
      .values()
      .filter(_ != TickerType.TICKER_ENUM_MAX)
      .map { t =>
        (("rocksdb_ticker_" + t.toString.toLowerCase, Map.empty[String, String]), refs.stats.getTickerCount(t).toDouble)
      }
      .toMap

    val histograms = HistogramType
      .values()
      .filter(_ != HistogramType.HISTOGRAM_ENUM_MAX)
      .flatMap { h =>
        val hist = refs.stats.getHistogramData(h)
        val name = "rocksdb_hist_" + h.toString.toLowerCase
        List(
          ((name, Map("component" -> "sum")), hist.getSum.toDouble),
          ((name, Map("component" -> "count")), hist.getCount.toDouble),
          ((name, Map("component" -> "max")), hist.getMax),
          ((name, Map("component" -> "min")), hist.getMin),
          ((name, Map("component" -> "median")), hist.getMedian),
          ((name, Map("component" -> "p95")), hist.getPercentile95),
          ((name, Map("component" -> "p99")), hist.getPercentile99),
          ((name, Map("component" -> "mean")), hist.getAverage),
          ((name, Map("component" -> "stddev")), hist.getStandardDeviation)
        )
      }
      .toMap

    val dbRef = refs.db

    val metrics = refs.columnHandleMap.flatMap {
      case (cf, cfHandle) =>
        val cfName = getColumnFamilyName(cf)
        val cfOptions = columnOptions(cf)
        val numLevels = cfOptions.numLevels()
        val blockSize = cfOptions.tableFormatConfig() match {
          case tableConfig: BlockBasedTableConfig =>
            tableConfig.blockSize().toDouble
          case _ =>
            0.0d
        }

        val numFilesAtLevels = (0 until numLevels)
          .map { i =>
            (
              ("rocksdb_cf_num_files_at_level", Map("cf" -> cfName, "level" -> i.toString)),
              dbRef.getProperty(cfHandle, s"rocksdb.num-files-at-level$i").toDouble
            )
          }

        val compressionRatioAtLevels = (0 until numLevels)
          .map { i =>
            (
              ("rocksdb_cf_compression_ratio_at_level", Map("cf" -> cfName, "level" -> i.toString)),
              dbRef.getProperty(cfHandle, s"rocksdb.compression-ratio-at-level$i").toDouble
            )
          }

        List(
          ("rocksdb_cf_block_based_table_block_size", Map("cf" -> cfName)) -> blockSize
        ) ++ numFilesAtLevels ++ compressionRatioAtLevels ++ cfMetrics.map {
          case (propertyName, metricName) =>
            (metricName, Map("cf" -> cfName)) -> dbRef.getProperty(cfHandle, s"rocksdb.$propertyName").toDouble
        }
    }

    tickers ++ histograms ++ metrics
  }

  private val InvalidIterator = Left(Array.emptyByteArray)

  private def maybeExactGet(constraints: List[KvdbKeyConstraint]): Option[Array[Byte]] = {
    if (constraints.size == 1 && constraints.head.operator == Operator.EQUAL) {
      Some(constraints.head.operand.toByteArray)
    }
    else None
  }

  private def doGet(iter: RocksIterator, constraints: List[KvdbKeyConstraint]): Either[Array[Byte], KvdbPair] = {
    if (constraints.isEmpty) {
      Left(Array.emptyByteArray)
    }
    else {
      val headConstraint = constraints.head
      val tailConstraints = constraints.tail

      val headOperand = headConstraint.operand.toByteArray
      val operator = headConstraint.operator

      operator match {
        case Operator.FIRST | Operator.LAST =>
          if (operator.isFirst) iter.seekToFirst() else iter.seekToLast()

          if (iter.isValid) {
            val key = iter.key()
            if (keySatisfies(key, tailConstraints)) Right(key -> iter.value())
            else Left(key)
          }
          else InvalidIterator

        case Operator.EQUAL =>
          iter.seek(headOperand)

          if (iter.isValid) {
            val key = iter.key()
            //noinspection CorrespondsUnsorted
            if (KeySerdes.isEqual(key, headOperand) && keySatisfies(key, tailConstraints)) Right(key -> iter.value())
            else Left(key)
          }
          else InvalidIterator

        case Operator.LESS_EQUAL | Operator.GREATER_EQUAL =>
          if (operator.isLessEqual) iter.seekForPrev(headOperand) else iter.seek(headOperand)

          if (iter.isValid) {
            val key = iter.key()
            if (keySatisfies(key, tailConstraints)) Right(key -> iter.value())
            else Left(key)
          }
          else InvalidIterator

        case Operator.LESS | Operator.GREATER =>
          if (operator.isLess) iter.seekForPrev(headOperand) else iter.seek(headOperand)

          if (iter.isValid) {
            val key = iter.key()

            if (KeySerdes.isEqual(key, headOperand)) {
              if (operator.isLess) iter.prev() else iter.next()

              if (iter.isValid) {
                val newKey = iter.key()
                if (keySatisfies(newKey, tailConstraints)) Right(newKey -> iter.value())
                else Left(newKey)
              }
              else Left(key)
            }
            else {
              if (keySatisfies(key, tailConstraints)) Right(key -> iter.value())
              else Left(key)
            }
          }
          else InvalidIterator

        case Operator.PREFIX =>
          iter.seek(headOperand)

          if (iter.isValid) {
            val key = iter.key()
            if (KeySerdes.isPrefix(headOperand, key) && keySatisfies(key, tailConstraints)) Right(key -> iter.value())
            else Left(key)
          }
          else InvalidIterator

        case Operator.Unrecognized(value) =>
          throw new IllegalArgumentException(s"""Got Operator.Unrecognized($value)""")
      }
    }
  }

  private def constraintsNeedTotalOrder(constraints: List[KvdbKeyConstraint]): Boolean = {
    constraints.headOption.exists {
      _.operator match {
        case Operator.LESS_EQUAL | Operator.LESS => true
        case _ => false
      }
    }
  }

  override def getTask[Col <: CF](column: Col, constraintList: KvdbKeyConstraintList): Task[Option[KvdbPair]] = {
    ioTask(references.map { refs =>
      val db = refs.db
      val columnHandle = refs.getColumnHandle(column)
      val options = newReadOptions()
      val constraints = constraintList.constraints

      maybeExactGet(constraints) match {
        case Some(key) =>
          Option(db.get(columnHandle, key)).map(value => key -> value)
        case None =>
          if (constraintsNeedTotalOrder(constraints)) {
            val _ = options.setTotalOrderSeek(true)
          }
          val iter = db.newIterator(columnHandle, options)

          try {
            doGet(iter, constraints).toOption
          }
          finally {
            options.close()
            iter.close()
          }
      }
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
    ioTask(references.map { refs =>
      val db = refs.db
      val columnHandle = refs.getColumnHandle(column)
      val options = newReadOptions()
      if (requests.exists(r => constraintsNeedTotalOrder(r.constraints))) {
        val _ = options.setTotalOrderSeek(true)
      }
      val iter = db.newIterator(columnHandle, options)

      try {
        requests.map { r =>
          maybeExactGet(r.constraints) match {
            case Some(key) =>
              Option(db.get(columnHandle, key)).map(value => key -> value)
            case None =>
              doGet(iter, r.constraints).toOption
          }
        }
      }
      finally {
        options.close()
        iter.close()
      }
    })
  }

  override def batchGetRangeTask[Col <: CF](column: Col, ranges: Seq[KvdbKeyRange]): Task[Seq[List[KvdbPair]]] = {
    ZIO
      .foreachPar(ranges) { range =>
        getRangeTask(column, range)
      }
  }

  override def estimateCount[Col <: CF](column: Col): Task[Long] = {
    ioTask(references.map { refs =>
      val columnHandle = refs.getColumnHandle(column)
      refs.db.getProperty(columnHandle, ESTIMATE_NUM_KEYS).toLong
    })
  }

  private def doPut(db: RocksDB, columnHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Unit = {
    db.put(columnHandle, key, value)
  }

  override def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    ioTask(references.map { refs => doPut(refs.db, refs.getColumnHandle(column), key, value) })
  }

  private def doDelete(db: RocksDB, columnHandle: ColumnFamilyHandle, key: Array[Byte]): Unit = {
    db.delete(columnHandle, key)
  }

  override def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit] = {
    ioTask(references.map { refs => doDelete(refs.db, refs.getColumnHandle(column), key) })
  }

  private def determineDeletePrefixRange(
    db: RocksDB,
    columnHandle: ColumnFamilyHandle,
    prefix: Array[Byte]
  ): (Long, Option[Array[Byte]], Option[Array[Byte]]) = {
    val iter = db.newIterator(columnHandle)

    try {
      iter.seek(prefix)

      if (iter.isValid) {
        val firstKey = iter.key

        if (KeySerdes.isPrefix(prefix, firstKey)) {
          val (count, lastKey) = Iterator
            .continually {
              iter.next()
              iter.isValid
            }
            .takeWhile(identity)
            .map(_ => iter.key)
            .takeWhile(KeySerdes.isPrefix(prefix, _))
            .foldLeft((1L, firstKey)) {
              case ((c, _), k) =>
                (c + 1, k)
            }

          (count, Some(firstKey), Some(lastKey))
        }
        else (0L, None, None)
      }
      else (0L, None, None)
    }
    finally {
      iter.close()
    }
  }

  override def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long] = {
    ioTask(references.map { refs =>
      val columnHandle = refs.getColumnHandle(column)
      val db = refs.db
      val writeBatch = new WriteBatch()

      val count = doDeletePrefix(db, columnHandle, writeBatch, prefix)

      try {
        if (count > 0) {
          val writeOptions = newWriteOptions()

          try {
            db.write(writeOptions, writeBatch)
          }
          finally {
            writeOptions.close()
          }
        }
        count
      }
      finally {
        writeBatch.close()
      }
    })
  }

  private def doDeletePrefix(
    db: RocksDB,
    columnHandle: ColumnFamilyHandle,
    writeBatch: WriteBatch,
    prefix: Array[Byte]
  ): Long = {
    determineDeletePrefixRange(db, columnHandle, prefix) match {
      case (c, Some(firstKey), Some(lastKey)) =>
        if (c > 1) {
          writeBatch.deleteRange(columnHandle, firstKey, lastKey)
        }
        writeBatch.delete(columnHandle, lastKey)
        c
      case _ =>
        0L
    }
  }

  override def watchKeySource[Col <: CF](column: Col, key: Array[Byte]): Source[Option[Array[Byte]], Future[Done]] = ???

  override def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = references
          .map {
            refs =>
              val init = () => {
                val columnHandle = refs.getColumnHandle(column)
                val db = refs.db
                val readOptions = newReadOptions()
                val fromConstraints = range.from
                val toConstraints = range.to

                if (constraintsNeedTotalOrder(fromConstraints)) {
                  val _ = readOptions.setTotalOrderSeek(true)
                }
                val iter = db.newIterator(columnHandle, readOptions)

                val close = () => {
                  readOptions.close()
                  iter.close()
                }

                doGet(iter, fromConstraints) match {
                  case Right(p) if keySatisfies(p._1, toConstraints) =>
                    val it = Iterator(p) ++ Iterator
                      .continually {
                        iter.next()
                        // scalastyle:off null
                        if (iter.isValid) iter.key() else null
                        // scalastyle:on null
                      }
                      .takeWhile(key => key != null && keySatisfies(key, toConstraints))
                      .map(key => key -> iter.value())

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
                      else s"There's no starting key satisfying constraints: [${fromConstraints.show}]"
                    }

                    Left(SeekFailure(message))
                }
              }

              Source
                .fromGraph(
                  new KvdbIterateSourceGraph(
                    init,
                    dbCloseSignal,
                    refs.ioDispatcher,
                    clientOptions.batchReadMaxBatchBytes
                  )
                )
          }

        rt.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  private def createTailSource[Col <: CF](
    refs: RocksdbContext[CF],
    column: Col,
    range: KvdbKeyRange
  ): Source[KvdbTailBatch, NotUsed] = {
    val init = () => {
      val columnHandle = refs.getColumnHandle(column)
      val db = refs.db

      val fromConstraints = range.from
      val toConstraints = range.to

      // scalastyle:off null
      var lastKey: Array[Byte] = null
      var readOptions: ReadOptions = null
      var iter: RocksIterator = null

      val close = () => {
        if (readOptions ne null) {
          readOptions.close()
          readOptions = null
        }
        if (iter ne null) {
          iter.close()
          iter = null
        }
      }
      // scalastyle:on null

      val createIterator = () => {
        readOptions = newReadOptions()
        iter = db.newIterator(columnHandle, readOptions)

        val headConstraints = {
          if (lastKey == null) fromConstraints
          else KvdbKeyConstraint(Operator.GREATER, ProtoByteString.copyFrom(lastKey)) :: toConstraints
        }

        val head = doGet(iter, headConstraints) match {
          case Right(v) if keySatisfies(v._1, toConstraints) => Iterator.single(v)
          case _ => Iterator.empty
        }

        if (head.nonEmpty) {
          val tail = Iterator
            .continually {
              iter.next()
              // scalastyle:off null
              if (iter.isValid) iter.key() else null
              // scalastyle:on null
            }
            .takeWhile(key => key != null && keySatisfies(key, toConstraints))
            .map(key => key -> iter.value())

          (head ++ tail).map { p =>
            lastKey = p._1 // Side-effecting
            p
          }
        }
        else head
      }

      KvdbTailSourceGraph.Refs(createIterator, clear = close, close = close)
    }

    Source
      .fromGraph(
        new KvdbTailSourceGraph(
          init,
          dbCloseSignal,
          refs.ioDispatcher,
          clientOptions.batchReadMaxBatchBytes,
          clientOptions.tailPollingMaxInterval,
          clientOptions.tailPollingBackoffFactor
        )
      )
  }

  override def concurrentTailSource[Col <: CF](
    column: Col,
    ranges: List[KvdbKeyRange]
  ): Source[KvdbIndexedTailBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = references
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

        rt.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  override def tailSource[Col <: CF](column: Col, range: KvdbKeyRange): Source[KvdbTailBatch, NotUsed] = {
    Source
      .lazyFuture(() => {
        val task = references
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

        rt.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
  }

  override def transactionTask(actions: Seq[TransactionWrite]): Task[Unit] = {
    ioTask(references.map { refs =>
      val db = refs.db
      val writeBatch = new WriteBatch()

      try {
        actions.foreach {
          case TransactionPut(columnId, key, value) =>
            writeBatch.put(refs.getColumnHandle(columnFamilyWithId(columnId).get), key, value)

          case TransactionDelete(columnId, key) =>
            writeBatch.delete(refs.getColumnHandle(columnFamilyWithId(columnId).get), key)

          case TransactionDeleteRange(columnId, fromKey, toKey) =>
            writeBatch.deleteRange(
              refs.getColumnHandle(columnFamilyWithId(columnId).get),
              fromKey,
              toKey
            )

          case _: TransactionMutateAdd => ???
          case _: TransactionMutateMin => ???
          case _: TransactionMutateMax => ???
        }

        val writeOptions = newWriteOptions()

        try {
          db.write(writeOptions, writeBatch)
        }
        finally {
          writeOptions.close()
        }
      }
      finally {
        writeBatch.close()
      }
    })
  }

  override def conditionalTransactionTask(
    reads: List[TransactionGet],
    condition: List[Option[(Array[Byte], Array[Byte])]] => Boolean,
    actions: Seq[TransactionWrite]
  ): Task[Unit] = {
    ioTask(references.flatMap { refs =>
      val db = refs.txDb.getOrElse(throw InvalidKvdbArgumentException(
        s"Database was opened as read-only, OptimisticTransactionDB is not available"
      ))

      val writeOptions = newWriteOptions()
      val tx = db.beginTransaction(writeOptions)

      try {
        val pairs = for (read <- reads) yield {
          val columnHandle = refs.getColumnHandle(columnFamilyWithId(read.columnId).get)
          val key = read.key
          val options = newReadOptions()

          val value =
            try {
              tx.getForUpdate(options, columnHandle, key, true)
            }
            finally {
              options.close()
            }

          if (value != null) Some(key -> value)
          else None
        }

        val okToWrite = condition(pairs)

        if (okToWrite) {
          actions.foreach {
            case TransactionPut(columnId, key, value) =>
              tx.put(refs.getColumnHandle(columnFamilyWithId(columnId).get), key, value)

            case TransactionDelete(columnId, key) =>
              tx.delete(refs.getColumnHandle(columnFamilyWithId(columnId).get), key)

            case TransactionDeleteRange(_, _, _) =>
              throw UnsupportedKvdbOperationException(
                "TransactionDeleteRange is not yet supported in conditionalTransactionTask"
              )

            case _: TransactionMutateAdd => ???
            case _: TransactionMutateMin => ???
            case _: TransactionMutateMax => ???
          }

          try {
            tx.commit()
            Task.succeed(())
          }
          catch {
            case ex: RocksDBException if ex.getStatus.getCode == org.rocksdb.Status.Code.Busy =>
              Task.fail(ConditionalTransactionFailedException("Writes conflicted"))
            case NonFatal(ex) =>
              Task.fail(ConditionalTransactionFailedException(s"Commit failed with ${ex.getMessage}"))
          }
        }
        else {
          Task.fail(ConditionalTransactionFailedException("Condition returns false"))
        }
      }
      finally {
        tx.close()
        writeOptions.close()
      }
    })
  }

  def compactRange[Col <: CF](column: Col): Task[Unit] = {
    ioTask(references.map { refs =>
      val db = refs.db
      val columnHandle = refs.getColumnHandle(column)
      db.compactRange(columnHandle)
    })
  }

  override def dropColumnFamily[Col <: CF](column: Col): Task[Unit] = {
    ioTask(references.map { refs =>
      logger.info(s"Dropping column family: ${getColumnFamilyName(column)}")
      refs.db.dropColumnFamily(refs.getColumnHandle(column))
    })
  }
}
