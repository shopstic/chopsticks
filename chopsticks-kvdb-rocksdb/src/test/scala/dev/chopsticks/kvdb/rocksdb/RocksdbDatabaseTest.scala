package dev.chopsticks.kvdb.rocksdb

import dev.chopsticks.fp.{AkkaApp, AkkaEnv}
import dev.chopsticks.kvdb.TestDatabase.{DefaultCf, LookupCf, TestCf, TestDb}
import dev.chopsticks.kvdb.codec.primitive._
import dev.chopsticks.kvdb.rocksdb.RocksdbColumnFamilyConfig.{PointLookupPattern, PrefixedScanPattern}
import dev.chopsticks.kvdb.util.KvdbUtils.KvdbAlreadyClosedException
import dev.chopsticks.kvdb.{ColumnFamilySet, KvdbDatabaseTest}
import org.rocksdb.CompressionType
import org.scalatest.Assertion
import squants.information.InformationConversions._
import zio.{RIO, ZIO}
import eu.timepit.refined.auto._

final class RocksdbDatabaseTest extends KvdbDatabaseTest {
  protected object defaultCf extends DefaultCf {
    override lazy val id: String = "default"
  }
  protected object lookupCf extends LookupCf
  private val cfs = ColumnFamilySet[TestCf].of(defaultCf).and(lookupCf)
  private val cfConfigMap = RocksdbColumnFamilyConfigMap[TestCf] of (
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

  protected val runTest: (TestDb => RIO[AkkaApp.Env, Assertion]) => RIO[AkkaApp.Env, Assertion] =
    (test: TestDb => RIO[AkkaApp.Env, Assertion]) => {
      KvdbDatabaseTest.withTempDir { dir =>
        ZIO
          .access[AkkaEnv] { implicit env =>
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
          .bracket(
            db =>
              db.closeTask().catchAll {
                case _: KvdbAlreadyClosedException => ZIO.unit
                case t => ZIO.die(t)
              }
          ) { db =>
            test(db)
          }
      }
    }
}
