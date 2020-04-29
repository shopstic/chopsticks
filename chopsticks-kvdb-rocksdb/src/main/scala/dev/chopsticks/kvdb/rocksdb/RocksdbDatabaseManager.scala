package dev.chopsticks.kvdb.rocksdb

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{ArrayList => JavaArrayList}

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import dev.chopsticks.kvdb.rocksdb.RocksdbDatabase.{Config, DEFAULT_COLUMN_NAME}
import dev.chopsticks.kvdb.rocksdb.RocksdbUtils.OptionsFileSection
import dev.chopsticks.kvdb.util.KvdbCloseSignal
import dev.chopsticks.kvdb.util.KvdbException.{
  InvalidKvdbArgumentException,
  KvdbAlreadyClosedException,
  UnoptimizedKvdbOperationException
}
import dev.chopsticks.kvdb.{ColumnFamily, KvdbMaterialization}
import eu.timepit.refined.auto._
import org.rocksdb._
import zio.blocking.{blocking, Blocking}
import zio.clock.Clock
import zio.{RIO, Schedule, Task, UIO, ZIO, ZManaged}

import scala.jdk.CollectionConverters._
import scala.util.Failure

object RocksdbDatabaseManager {
  private val KvdbClosedException = KvdbAlreadyClosedException("Database was already closed")

  def byteArrayToString(bytes: Array[Byte]): String = {
    new String(bytes, UTF_8)
  }

  final case class RocksdbContext[CF <: ColumnFamily[_, _]](
    txDb: OptimisticTransactionDB,
    db: RocksDB,
    columnHandleMap: Map[CF, ColumnFamilyHandle],
    columnPrefixExtractorOptionMap: Map[CF, String],
    stats: Statistics,
    ioDispatcher: String
  ) {
    val dbCloseSignal = new KvdbCloseSignal
    private val isClosed = new AtomicBoolean(false)

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

    def obtain(): ZIO[Any, Throwable, RocksdbContext[CF]] = {
      UIO(isClosed.get)
        .flatMap { isClosed =>
          if (isClosed) Task.fail(KvdbClosedException)
          else ZIO.succeed(this)
        }
    }

    def close(): ZIO[Blocking with Clock, Throwable, Unit] = {
      import zio.duration._

      for {
        _ <- Task(isClosed.compareAndSet(false, true)).flatMap { isClosed =>
          if (isClosed) Task.unit
          else Task.fail(KvdbClosedException)
        }
        _ <- Task(dbCloseSignal.tryComplete(Failure(KvdbClosedException)))
        _ <- Task(dbCloseSignal.hasNoListeners)
          .repeat(Schedule.fixed(100.millis).untilInput(identity))
        _ <- blocking(Task {
          stats.close()
          columnHandleMap.values.foreach { c => txDb.flush(new FlushOptions().setWaitForFlush(true), c) }
          columnHandleMap.foreach(_._2.close())
          txDb.flushWal(true)
          txDb.closeE()
        })
      } yield ()
    }
  }

  def apply[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]](
    materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
    config: Config
  ) = new RocksdbDatabaseManager(materialization, config)
}

import dev.chopsticks.kvdb.rocksdb.RocksdbDatabaseManager._

final class RocksdbDatabaseManager[BCF[A, B] <: ColumnFamily[A, B], CFS <: BCF[_, _]] private (
  materialization: KvdbMaterialization[BCF, CFS] with RocksdbMaterialization[BCF, CFS],
  config: Config
) extends StrictLogging {
  type CF = BCF[_, _]

  def managedContext: ZManaged[Blocking with Clock, Throwable, RocksdbContext[CF]] = {
    ZManaged.make(open())(_.close().orDie)
  }

  private def columnFamilyWithName(name: String): Option[CF] =
    materialization.columnFamilySet.value.find(cf => getColumnFamilyName(cf) == name)

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

  private def getColumnFamilyName(cf: BCF[_, _]): String = {
    if (cf == materialization.defaultColumnFamily) DEFAULT_COLUMN_NAME else cf.id
  }

  private def syncColumnFamilies(descriptors: List[ColumnFamilyDescriptor], existingColumnNames: Set[String]): Unit = {
    if (existingColumnNames.isEmpty) {
      logger.info("Opening database for the first time, creating column families...")
      val nonDefaultColumns = descriptors
        .filterNot(_.getName.sameElements(RocksDB.DEFAULT_COLUMN_FAMILY))

      if (nonDefaultColumns.nonEmpty) {
        val db = OptimisticTransactionDB.open(new Options().setCreateIfMissing(true), config.path)

        val columns = nonDefaultColumns.map { d => db.createColumnFamily(d) }

        columns.foreach(_.close())
        db.close()
      }
    }
    else {
      val handles = new JavaArrayList[ColumnFamilyHandle]
      val existingDescriptor = descriptors.filter(d => existingColumnNames.contains(byteArrayToString(d.getName)))
      val toCreateDescriptors = descriptors.filter(d => !existingColumnNames.contains(byteArrayToString(d.getName)))

      val db = OptimisticTransactionDB.open(new DBOptions(), config.path, existingDescriptor.asJava, handles)

      val newHandles = toCreateDescriptors.map { d =>
        logger.info(s"Creating column family: ${byteArrayToString(d.getName)}")
        db.createColumnFamily(d)
      }

      handles.asScala.foreach(_.close())
      newHandles.foreach(_.close())
      db.close()
    }
  }

  private def open(): ZIO[Blocking, Throwable, RocksdbContext[CF]] = {
    blocking(Task {
      RocksDB.loadLibrary()

      val columnOptions: Map[BCF[_, _], ColumnFamilyOptions] = materialization.columnFamilyConfigMap.map
      val columnNames = columnOptions.keys.map(getColumnFamilyName).toSet
      val columnOptionsAsList = columnOptions.toList
      val descriptors = columnOptionsAsList.map {
        case (cf, colOptions) =>
          new ColumnFamilyDescriptor(
            getColumnFamilyName(cf).getBytes(UTF_8),
            colOptions.setDisableAutoCompactions(config.startWithBulkInserts)
          )
      }

      val dbOptions: DBOptions = {
        val coreCount = Runtime.getRuntime.availableProcessors()
        val options = new DBOptions()

        val totalWriteBufferSize = columnOptions.values.map(_.writeBufferSize()).sum

        val tunedOptions = options
          .setIncreaseParallelism(coreCount)
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

      val exists = File(config.path + "/CURRENT").exists

      def openKvdb() = {
        val handles = new JavaArrayList[ColumnFamilyHandle]

        val _ = dbOptions
          .setStatsDumpPeriodSec(60)
          .setStatistics(new Statistics())

        val db = {
          OptimisticTransactionDB.open(dbOptions.setCreateIfMissing(true), config.path, descriptors.asJava, handles)
        }

        val columnHandles = handles.asScala.toList

        val columnHandleMap = columnOptionsAsList.map(_._1).zip(columnHandles).toMap

        (db, columnHandleMap, dbOptions.statistics())
      }

      def listExistingColumnNames(): List[String] = {
        RocksDB.listColumnFamilies(new Options(), config.path).asScala.map(byteArrayToString).toList
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

            RocksdbContext[CF](
              txDb = db,
              db = db.getBaseDB,
              columnHandleMap = columnHandleMap,
              columnPrefixExtractorOptionMap = columnHasPrefixExtractorMap,
              stats = stats,
              ioDispatcher = config.ioDispatcher
            )
          }
    }
  }

}
