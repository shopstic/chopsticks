package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.fp.AkkaApp
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestDb, TestDbCf}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.{PointLookupPattern, PrefixedScanPattern}
import dev.chopsticks.kvdb.util.KvdbTestUtils
import dev.chopsticks.kvdb.util.KvdbUtils.KvdbAlreadyClosedException
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest}
import eu.timepit.refined.auto._
import org.rocksdb.CompressionType
import squants.information.InformationConversions._
import zio.{ZIO, ZManaged}

object RocksdbDatabaseTest {
  object defaultCf extends DefaultCf {
    override lazy val id: String = "default"
  }
  object lookupCf extends LookupCf

  private val cfs = ColumnFamilySet[TestDbCf] of defaultCf and lookupCf
  private val cfConfigMap = RocksdbColumnFamilyConfigMap[TestDbCf] of (
    defaultCf,
    RocksdbColumnFamilyConfig(
      memoryBudget = 1.mib,
      blockCache = 1.mib,
      blockSize = 8.kib,
      readPattern = PrefixedScanPattern(1),
      compression = CompressionType.NO_COMPRESSION
    )
  ) and (
    lookupCf,
    RocksdbColumnFamilyConfig(
      memoryBudget = 1.mib,
      blockCache = 1.mib,
      blockSize = 8.kib,
      readPattern = PointLookupPattern,
      compression = CompressionType.NO_COMPRESSION
    )
  )

  val managedDb: ZManaged[AkkaApp.Env, Throwable, TestDb] = {
    for {
      dir <- KvdbTestUtils.managedTempDir
      db <- ZManaged.make {
        ZIO
          .access[AkkaApp.Env] { implicit env =>
            RocksdbDatabase(
              cfs,
              cfConfigMap,
              dir.pathAsString,
              readOnly = false,
              startWithBulkInserts = false,
              checksumOnRead = true,
              syncWriteBatch = true,
              useDirectIo = true,
              ioDispatcher = "dev.chopsticks.kvdb.test-db-io-dispatcher"
            )
          }
      } {
        _.closeTask().catchAll {
          case _: KvdbAlreadyClosedException => ZIO.unit
          case e => ZIO.die(e)
        }
      }
    } yield db
  }
}

final class RocksdbDatabaseTest extends KvdbDatabaseTest {
  protected val defaultCf = RocksdbDatabaseTest.defaultCf
  protected val lookupCf = RocksdbDatabaseTest.lookupCf
  protected val managedDb = RocksdbDatabaseTest.managedDb
}
