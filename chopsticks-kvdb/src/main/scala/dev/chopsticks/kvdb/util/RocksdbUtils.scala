package dev.chopsticks.kvdb.util

import java.util.Properties

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.rocksdb._
import zio.blocking._
import zio.{Task, RIO}

object RocksdbUtils extends StrictLogging {
  val LOOKUP_COLUMN_FAMILY_NAME: String = "lookup"

  final case class DbHandles(db: RocksDB, handles: List[ColumnFamilyHandle], stats: Statistics)

  def createDbOptions(enableStats: Boolean = false): DBOptions = {
//    val coresCount = Runtime.getRuntime.availableProcessors()
    val options = new DBOptions()
      .setKeepLogFileNum(5)
      .setMaxOpenFiles(-1)
//      .setDbWriteBufferSize(4.gib)
//      .setMaxBackgroundCompactions(coresCount)
//      .setMaxTotalWalSize(2.gib)
//      .setWalSizeLimitMB(0)
//      .setWalTtlSeconds(0)
//      .setInfoLogLevel(InfoLogLevel.INFO_LEVEL)

    if (enableStats) {
      val stats = new Statistics()
//      stats.setStatsLevel(StatsLevel.ALL)

      options
        .setStatsDumpPeriodSec(30)
        .setStatistics(stats)
    }
    else options
  }

  def findLatestOptionsFile(dbPath: String): RIO[Blocking, File] = {
    blocking(Task {
      val fileList = File(dbPath).list
      val prefix = "OPTIONS-"
      val optionsFiles = fileList.filter(_.name.startsWith(prefix))

      if (optionsFiles.isEmpty) {
        logger.warn(s"List of files: $fileList")
        throw new RuntimeException("No options file found")
      }

      optionsFiles.maxBy(_.name.drop(prefix.length).toInt)
    })
  }

  final case class OptionsFileSection(name: String, values: Map[String, String] = Map.empty[String, String])

  private val optionsSectionRegex = """^\[(.+)\]$""".r
  private val optionsValueRegex = """^([^=]+)=(.+)$""".r

  def parseOptions(lines: Traversable[String]): List[OptionsFileSection] = {
    val (all, lastSection) = lines
      .map(_.trim)
      .foldLeft[(List[OptionsFileSection], Option[OptionsFileSection])]((List.empty[OptionsFileSection], None)) {
        case (state @ (list, section), line) =>
          line match {
            case optionsSectionRegex(name, _*) =>
              val s = Some(OptionsFileSection(name))

              if (section.nonEmpty) (list :+ section.get, s)
              else (list, s)

            case optionsValueRegex(key, value, _*) =>
              (list, section.map { s =>
                s.copy(values = s.values.updated(key, value))
              })
            case _ => state
          }
      }

    lastSection.map(s => all :+ s).getOrElse(all)
  }

  def propertiesFromMap(map: Map[String, String]): Properties = {
    (new Properties /: map) {
      case (a, (k, v)) =>
        val _ = a.put(k, v)
        a
    }
  }

  def estimateCount(implicit db: RocksDB, column: Option[ColumnFamilyHandle] = None): Long = {
    column
      .map { c =>
        db.getProperty(c, "rocksdb.estimate-num-keys").toLong
      }
      .getOrElse(db.getProperty("rocksdb.estimate-num-keys").toLong)
  }

  def close(handles: DbHandles): Unit = {
    try {
      handles.db.flush(new FlushOptions().setWaitForFlush(true))
    } catch {
      case e: RocksDBException if e.getMessage.contains("read only") =>
      case e: Throwable => throw e
    }
    handles.handles.foreach(_.close())
    handles.db.close()
  }
}
