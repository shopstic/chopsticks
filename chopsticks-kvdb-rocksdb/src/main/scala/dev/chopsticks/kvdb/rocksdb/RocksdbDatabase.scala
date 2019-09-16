package dev.chopsticks.kvdb.rocksdb

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{ArrayList => JavaArrayList}

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.{Merge, Source}
import better.files.File
import cats.syntax.show._
import com.google.protobuf.{ByteString => ProtoByteString}
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.fp.AkkaEnv
import dev.chopsticks.kvdb.ColumnFamilyTransactionBuilder.{
  TransactionAction,
  TransactionDelete,
  TransactionDeleteRange,
  TransactionPut
}
import dev.chopsticks.kvdb.KvdbDatabase.keySatisfies
import dev.chopsticks.kvdb.KvdbMaterialization.DuplicatedColumnFamilyIdsException
import dev.chopsticks.kvdb.codec.KeyConstraints.Implicits._
import dev.chopsticks.kvdb.codec.KeySerdes
import dev.chopsticks.kvdb.proto.KvdbKeyConstraint.Operator
import dev.chopsticks.kvdb.proto._
import dev.chopsticks.kvdb.rocksdb.RocksdbUtils.OptionsFileSection
import dev.chopsticks.kvdb.util.KvdbAliases._
import dev.chopsticks.kvdb.util.KvdbException._
import dev.chopsticks.kvdb.util._
import dev.chopsticks.kvdb.{ColumnFamily, KvdbDatabase, KvdbMaterialization}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import org.rocksdb._
import pureconfig.ConfigConvert
import zio.blocking._
import zio.clock.Clock
import zio.{RIO, Task, ZIO, ZSchedule}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.Failure

object RocksdbDatabase extends StrictLogging {
  final val DEFAULT_COLUMN_NAME: String = new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8)

  private def byteArrayToString(bytes: Array[Byte]): String = {
    new String(bytes, UTF_8)
  }

  private val ESTIMATE_NUM_KEYS = "rocksdb.estimate-num-keys"

  private val KvdbClosedException = KvdbAlreadyClosedException("Database was already closed")

  final case class KvdbReferences[CF <: ColumnFamily[_, _]](
    db: RocksDB,
    columnHandleMap: Map[CF, ColumnFamilyHandle],
    columnPrefixExtractorOptionMap: Map[CF, String],
    stats: Statistics
  ) {
    def getColumnHandle[Col <: CF](column: Col): ColumnFamilyHandle = {
      columnHandleMap.getOrElse(
        column,
        throw InvalidKvdbArgumentException(
          s"Column family: $column doesn't exist in columnHandleMap: $columnHandleMap"
        )
      )
    }

    def validateColumnPrefixSeekOperation[Col <: CF](column: Col, prefix: Array[Byte]): Option[Throwable] = {
      if (prefix.nonEmpty) {
        val prefixString = byteArrayToString(prefix)

        columnPrefixExtractorOptionMap(column) match {
          case "nullptr" | "rocksdb.Noop" =>
            Some(
              UnoptimizedKvdbOperationException(
                s"Iterating with prefix $prefixString on column ${column.id} but this column " +
                  s"is not optimized for prefix seeking (no prefix extractor)"
              )
            )
          case o if o.contains("rocksdb.FixedPrefix.") =>
            val length = prefix.length
            val configuredLength = o.drop("rocksdb.FixedPrefix.".length).toInt
            if (length < configuredLength) {
              Some(
                UnoptimizedKvdbOperationException(
                  s"Iterating with prefix $prefixString (length = $length) on column ${column.id} but the configured " +
                    s"prefix extractor min length is $configuredLength"
                )
              )
            }
            else None
          case _ =>
            None
        }
      }
      else None
    }
  }

  private def readColumnFamilyOptionsFromDisk(dbPath: String): RIO[Blocking, Map[String, Map[String, String]]] = {
    blocking(Task {
      val fileList = File(dbPath).list
      val prefix = "OPTIONS-"
      val optionsFiles = fileList.filter(_.name.startsWith(prefix))

      if (optionsFiles.isEmpty) {
        throw new RuntimeException(s"No options file found. List of files: ${fileList.mkString(", ")}")
      }

      optionsFiles.maxBy(_.name.drop(prefix.length).toInt)
    }).map { f =>
        logger.info(s"Latest db options file: ${f.pathAsString}")
        f
      }
      .map(_.lines)
      .map(RocksdbUtils.parseOptions)
      .map { sections =>
        val columnFamilyRegex = """CFOptions "([^"]+)"""".r
        sections.collect {
          case OptionsFileSection(columnFamilyRegex(name, _*), m) =>
            (name, m)
        }.toMap
      }
  }

  final case class Config(
    path: NonEmptyString,
    readOnly: Boolean,
    startWithBulkInserts: Boolean,
    checksumOnRead: Boolean,
    syncWriteBatch: Boolean,
    useDirectIo: Boolean,
    ioDispatcher: NonEmptyString
  )

  object Config {
    import dev.chopsticks.util.config.PureconfigConverters._
    import eu.timepit.refined.pureconfig._
    //noinspection TypeAnnotation
    implicit val configConvert = ConfigConvert[Config]
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
    config: Config
  ): ZIO[AkkaEnv, DuplicatedColumnFamilyIdsException, RocksdbDatabase[BCF, CFS]] = {
    RocksdbMaterialization.validate(materialization) match {
      case Left(ex) => ZIO.fail(ex)
      case Right(_) =>
        ZIO.access[AkkaEnv] { implicit env =>
          new RocksdbDatabase[BCF, CFS](materialization, config)
        }
    }
  }
}

final class RocksdbDatabase[BCF[A, B] <: ColumnFamily[A, B], +CFS <: BCF[_, _]] private (
  val materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
  config: RocksdbDatabase.Config
)(implicit env: AkkaEnv)
    extends KvdbDatabase[BCF, CFS]
    with StrictLogging {

  import RocksdbDatabase._

  private val akkaEnv: AkkaEnv.Service = env.akka

  val isLocal: Boolean = true

  private val columnOptions: Map[CF, ColumnFamilyOptions] = materialization.columnFamilyConfigMap.map

  private val coreCount: Int = Runtime.getRuntime.availableProcessors()
  private lazy val dbOptions: DBOptions = {
    val options = new DBOptions()

    val totalWriteBufferSize = columnOptions.values.map(_.writeBufferSize()).sum

    val tunedOptions = options
      .setIncreaseParallelism(coreCount)
      .setMaxBackgroundFlushes(coreCount)
      .setMaxBackgroundCompactions(coreCount)
      .setMaxBackgroundJobs(coreCount)
      .setMaxSubcompactions(coreCount)
      .setMaxOpenFiles(-1)
      .setKeepLogFileNum(3)
      .setMaxTotalWalSize(totalWriteBufferSize * 8)

    if (config.useDirectIo) {
      tunedOptions
        .setUseDirectIoForFlushAndCompaction(true)
        .setUseDirectReads(true)
        .setCompactionReadaheadSize(2 * 1024 * 1024)
        .setWritableFileMaxBufferSize(1024 * 1024)
    }
    else tunedOptions
  }

  protected def logPath: Option[String] = None

  private def getColumnFamilyName(cf: CF): String = {
    if (cf == materialization.defaultColumnFamily) DEFAULT_COLUMN_NAME else cf.id
  }

  private def columnFamilyWithName(name: String): Option[CF] =
    materialization.columnFamilySet.value.find(cf => getColumnFamilyName(cf) == name)

  private def newReadOptions(): ReadOptions = {
    val o = new ReadOptions()
    if (config.checksumOnRead) o.setVerifyChecksums(config.checksumOnRead) else o
  }

  private def newWriteOptions(): WriteOptions = {
    val o = new WriteOptions()
    if (config.syncWriteBatch) o.setSync(true) else o
  }

  private def syncColumnFamilies(descriptors: List[ColumnFamilyDescriptor], existingColumnNames: Set[String]): Unit = {
    if (existingColumnNames.isEmpty) {
      logger.info("Opening database for the first time, creating column families...")
      val nonDefaultColumns = descriptors
        .filterNot(_.getName.sameElements(RocksDB.DEFAULT_COLUMN_FAMILY))

      if (nonDefaultColumns.nonEmpty) {
        val db = RocksDB.open(new Options().setCreateIfMissing(true), config.path)

        val columns = nonDefaultColumns.map { d =>
          db.createColumnFamily(d)
        }

        columns.foreach(_.close())
        db.close()
      }
    }
    else {
      val handles = new JavaArrayList[ColumnFamilyHandle]
      val existingDescriptor = descriptors.filter(d => existingColumnNames.contains(byteArrayToString(d.getName)))
      val toCreateDescriptors = descriptors.filter(d => !existingColumnNames.contains(byteArrayToString(d.getName)))

      val db = RocksDB.open(new DBOptions(), config.path, existingDescriptor.asJava, handles)

      val newHandles = toCreateDescriptors.map { d =>
        logger.info(s"Creating column family: ${byteArrayToString(d.getName)}")
        db.createColumnFamily(d)
      }

      handles.asScala.foreach(_.close())
      newHandles.foreach(_.close())
      db.close()
    }
  }

  private def listExistingColumnNames(): List[String] = {
    RocksDB.listColumnFamilies(new Options(), config.path).asScala.map(byteArrayToString).toList
  }

//  private val bulkInsertsLock = MVar.of[Task, Boolean](startWithBulkInserts).memoize

//  private lazy val ioEc = akkaEnv.actorSystem.dispatchers.lookup(config.ioDispatcher)
//  private lazy val ioZioExecutor = Executor.fromExecutionContext(1)(ioEc)
//  private lazy val blockingEnv = new Blocking {
//    val blocking: Blocking.Service[Any] = new Blocking.Service[Any] {
//      val blockingExecutor: ZIO[Any, Nothing, Executor] = UIO.succeed(ioZioExecutor)
//    }
//  }

  private val dbCloseSignal = new KvdbCloseSignal

  private lazy val _references = {
    val task = blocking(Task {
      RocksDB.loadLibrary()
      val columnNames = columnOptions.keys.map(getColumnFamilyName).toSet
      val columnOptionsAsList = columnOptions.toList
      val descriptors = columnOptionsAsList.map {
        case (cf, colOptions) =>
          new ColumnFamilyDescriptor(
            getColumnFamilyName(cf).getBytes(UTF_8),
            colOptions.setDisableAutoCompactions(config.startWithBulkInserts)
          )
      }

      val exists = File(config.path + "/CURRENT").exists

      if (!exists && config.readOnly)
        throw InvalidKvdbArgumentException(s"Opening database at ${config.path} as readyOnly but it doesn't exist")

      def openKvdb() = {
        val handles = new JavaArrayList[ColumnFamilyHandle]

        val _ = dbOptions
          .setStatsDumpPeriodSec(60)
          .setStatistics(new Statistics())

        val db = {
          if (config.readOnly) RocksDB.openReadOnly(dbOptions, config.path, descriptors.asJava, handles)
          else RocksDB.open(dbOptions.setCreateIfMissing(true), config.path, descriptors.asJava, handles)
        }

        val columnHandles = handles.asScala.toList

        val columnHandleMap = columnOptionsAsList.map(_._1).zip(columnHandles).toMap

        (db, columnHandleMap, dbOptions.statistics())
      }

      if (exists) {
        val existingColumnNames = listExistingColumnNames().toSet

        if (columnNames != existingColumnNames) {
          syncColumnFamilies(descriptors, existingColumnNames)

          val doubleCheckingColumnNames = listExistingColumnNames().toSet
          assert(
            columnNames == doubleCheckingColumnNames,
            s"Trying to open with $columnNames but existing columns are $doubleCheckingColumnNames"
          )
        }
        else {
          assert(
            columnNames == existingColumnNames,
            s"Trying to open with $columnNames but existing columns are $existingColumnNames"
          )
        }
      }
      else {
        syncColumnFamilies(descriptors, Set.empty[String])
      }

      openKvdb()
    }).flatMap {
      case (db, columnHandleMap, stats) =>
        readColumnFamilyOptionsFromDisk(config.path)
          .map { r =>
            val columnHasPrefixExtractorMap: Map[CF, String] = r.iterator.map {
              case (colName, colMap) =>
                val cf = columnFamilyWithName(colName).get
                val prefixExtractor = colMap("prefix_extractor")
                (cf, prefixExtractor)
            }.toMap

            KvdbReferences[CF](db, columnHandleMap, columnHasPrefixExtractorMap, stats)
          }
    }

    akkaEnv.unsafeRunToFuture(task.provide(KvdbIoThreadPool.blockingEnv))
  }

  private def ioTask[T](task: Task[T]): Task[T] = {
    task.lock(KvdbIoThreadPool.executor)
  }

  private val isClosed = new AtomicBoolean(false)

//  private val memoizedReferencesTask: ZIO[Any, Nothing, IO[Throwable, KvdbReferences[KvdbDef#BaseCol]]] = Task.fromFuture(_ => _references).memoize

  def references: Task[KvdbReferences[CF]] = Task(isClosed.get).flatMap { isClosed =>
    if (isClosed) Task.fail(KvdbClosedException)
    else {
      _references.value.fold(Task.fromFuture(_ => _references))(Task.fromTry(_))
    }
  }

  def openTask(): Task[Unit] = references.unit

  def compactTask(): Task[Unit] = references.flatMap { refs =>
    ioTask(Task {
      refs.columnHandleMap.values.foreach { col =>
        refs.db.compactRange(col)
      }
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
  ).map(n => (n, "rocksdb_cf_" + n.replaceAllLiterally("-", "_")))

  def statsTask: Task[Map[(String, Map[String, String]), Double]] = references.map { refs =>
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

  def getTask[Col <: CF](column: Col, constraintList: KvdbKeyConstraintList): Task[Option[KvdbPair]] = {
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
          } finally {
            options.close()
            iter.close()
          }
      }
    })
  }

  def batchGetTask[Col <: CF](
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
      } finally {
        options.close()
        iter.close()
      }
    })
  }

  def estimateCount[Col <: CF](column: Col): Task[Long] = {
    ioTask(references.map { refs =>
      val columnHandle = refs.getColumnHandle(column)
      refs.db.getProperty(columnHandle, ESTIMATE_NUM_KEYS).toLong
    })
  }

  private def doPut(db: RocksDB, columnHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Unit = {
    db.put(columnHandle, key, value)
  }

  def putTask[Col <: CF](column: Col, key: Array[Byte], value: Array[Byte]): Task[Unit] = {
    ioTask(references.map { refs =>
      doPut(refs.db, refs.getColumnHandle(column), key, value)
    })
  }

  private def doDelete(db: RocksDB, columnHandle: ColumnFamilyHandle, key: Array[Byte]): Unit = {
    db.delete(columnHandle, key)
  }

  def deleteTask[Col <: CF](column: Col, key: Array[Byte]): Task[Unit] = {
    ioTask(references.map { refs =>
      doDelete(refs.db, refs.getColumnHandle(column), key)
    })
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
    } finally {
      iter.close()
    }
  }

  def deletePrefixTask[Col <: CF](column: Col, prefix: Array[Byte]): Task[Long] = {
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
          } finally {
            writeOptions.close()
          }
        }
        count
      } finally {
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

  def iterateValuesSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbValueBatch, Future[NotUsed]] = {
    iterateSource(column, range)
      .map(_.map(_._2))
  }

  def iterateSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbBatch, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {
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
                          .map { d =>
                            s"Starting key: [$d] does not satisfy constraints: [${fromConstraints.show}]"
                          }
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
                .fromGraph(new KvdbIterateSourceGraph(init, dbCloseSignal, config.ioDispatcher))
          }

        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  def closeTask(): RIO[Clock, Unit] = {
    import zio.duration._

    for {
      refs <- references
      _ <- Task(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
        if (isClosed) Task.unit
        else Task.fail(KvdbClosedException)
      }
      _ <- Task(dbCloseSignal.tryComplete(Failure(KvdbClosedException)))
      _ <- Task(dbCloseSignal.hasNoListeners)
        .repeat(ZSchedule.fixed(100.millis).untilInput[Boolean](identity))
      _ <- ioTask(Task {
        val KvdbReferences(db, columnHandleMap, _, stats) = refs
        stats.close()
        columnHandleMap.values.foreach { c =>
          db.flush(new FlushOptions().setWaitForFlush(true), c)
        }
        columnHandleMap.foreach(_._2.close())
        db.flushWal(true)
        db.closeE()
      })
    } yield ()
  }

  private def createTailSource[Col <: CF](
    refs: KvdbReferences[CF],
    column: Col,
    range: KvdbKeyRange
  )(implicit clientOptions: KvdbClientOptions): Source[KvdbTailBatch, NotUsed] = {
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
      .fromGraph(new KvdbTailSourceGraph(init, dbCloseSignal, config.ioDispatcher))
  }

  def concurrentTailSource[Col <: CF](
    column: Col,
    ranges: List[KvdbKeyRange]
  )(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbIndexedTailBatch, Future[NotUsed]] = {
    Source
      .lazilyAsync(() => {
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
        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
      .addAttributes(Attributes.inputBuffer(1, 1))
  }

  def tailSource[Col <: CF](column: Col, range: KvdbKeyRange)(
    implicit clientOptions: KvdbClientOptions
  ): Source[KvdbTailBatch, Future[NotUsed]] = {

    Source
      .lazilyAsync(() => {
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

        akkaEnv.unsafeRunToFuture(task)
      })
      .flatMapConcat(identity)
  }

  def tailValuesSource[Col <: CF](
    column: Col,
    range: KvdbKeyRange
  )(implicit clientOptions: KvdbClientOptions): Source[KvdbTailValueBatch, Future[NotUsed]] = {
    tailSource(column, range)
      .map(_.map(_.map(_._2)))
  }

//  @SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
  def transactionTask(actions: Seq[TransactionAction]): Task[Unit] = {
    ioTask(references.map { refs =>
      val db = refs.db
      val writeBatch = new WriteBatch()

      try {
        actions.foreach {
          case TransactionPut(columnId, key, value) =>
            writeBatch.put(refs.getColumnHandle(columnFamilyWithId(columnId).get), key, value)

          case TransactionDelete(columnId, key, single) =>
            if (single) {
              writeBatch.singleDelete(refs.getColumnHandle(columnFamilyWithId(columnId).get), key)
            }
            else {
              writeBatch.delete(refs.getColumnHandle(columnFamilyWithId(columnId).get), key)
            }

          case TransactionDeleteRange(columnId, fromKey, toKey) =>
            writeBatch.deleteRange(
              refs.getColumnHandle(columnFamilyWithId(columnId).get),
              fromKey,
              toKey
            )
        }

        val writeOptions = newWriteOptions()

        try {
          db.write(writeOptions, writeBatch)
        } finally {
          writeOptions.close()
        }
      } finally {
        writeBatch.close()
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

  def dropColumnFamily[Col <: CF](column: Col): Task[Unit] = {
    ioTask(references.map { refs =>
      logger.info(s"Dropping column family: ${getColumnFamilyName(column)}")
      refs.db.dropColumnFamily(refs.getColumnHandle(column))
    })
  }
}
